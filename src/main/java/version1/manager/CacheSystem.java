package version1.manager;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.sql.*;
import java.util.*;
import java.util.Date;

import static org.apache.spark.sql.functions.*;

public class CacheSystem {
    private final SparkSession sparkSession;
    private Timestamp cachedResultTill;
    private MyProperties myProperties;

    public CacheSystem(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        this.myProperties = PropertiesManager.getProperties();
        cachedResultTill = new Timestamp(System.currentTimeMillis());

    }

    private void saveGranularities() {
        LogManager.logInfo("[Saving granularity to cache databases]");
        Set<MySQLTable.SchemaType> schemaTypes = myProperties.cacheMySQLTableSchemaTypeMap.keySet();
        for (MySQLTable.SchemaType schemaType : schemaTypes) {
            MySQLTable cacheMySQLTable = myProperties.cacheMySQLTableSchemaTypeMap.get(schemaType);
            Dataset<Row> granularityDataset = sparkSession.createDataFrame(myProperties.granularities, Granularity.class);
            granularityDataset.orderBy("displayPriority").write().mode(SaveMode.Overwrite).jdbc(cacheMySQLTable.getURL(), myProperties.GRANULARITY_TABLE_NAME_SUFFIX, cacheMySQLTable.getProperties());
        }
    }

    private void loadBitmaps() {
        LogManager.logInfo("[Loading bitmap from cache databases]");
        Set<MySQLTable> cacheMySQLTables = myProperties.cacheMySQLTableSensorMap.keySet();
        for (MySQLTable cacheMySQLTable : cacheMySQLTables) {
            try {
                Connection connection = DriverManager.getConnection(cacheMySQLTable.getURL(), cacheMySQLTable.getProperties());
                String tableName = cacheMySQLTable.getTableName() + "_" + myProperties.BITMAP_TABLE_NAME_SUFFIX;
                PreparedStatement preparedStatement = connection.prepareStatement(String.format("select %s,granularity,bitset from %s ", cacheMySQLTable.getSensorIdColumnName(), tableName));
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    String sensorId = resultSet.getString(cacheMySQLTable.getSensorIdColumnName());
                    String granularity = resultSet.getString("granularity");
                    Blob blob = resultSet.getBlob("bitset");
                    BitSet bitSet = BitSet.valueOf(blob.getBytes(1, (int) blob.length()));
                    myProperties.sensorMap.get(sensorId).bitmap.setBitset(granularity, bitSet);
                }
            } catch (SQLException e) {
                LogManager.logError("[" + this.getClass() + "][LoadBitmap]" + e.getMessage());
            }
        }
    }

    private void saveBitmaps() {
        LogManager.logInfo("[Saving bitmap to cache databases]");
        Set<MySQLTable> cacheMySQLTables = myProperties.cacheMySQLTableSensorMap.keySet();
        for (MySQLTable cacheMySQLTable : cacheMySQLTables) {
            assert cacheMySQLTable != null;
            try {
                Connection connection = DriverManager.getConnection(cacheMySQLTable.getURL(), cacheMySQLTable.getProperties());
                String tableName = cacheMySQLTable.getTableName() + "_" + myProperties.BITMAP_TABLE_NAME_SUFFIX;
//                int i = connection.prepareStatement("drop table if exists " + tableName).executeUpdate();
//                assert i == 0;
                int i = connection.prepareStatement(String.format("create table if not exists %s (%s varchar(200),granularity varchar(50), bitmapStartDate long,bitmapEndDate long, bitset mediumblob, CONSTRAINT PK_%s PRIMARY KEY (%s,granularity))", tableName, cacheMySQLTable.getSensorIdColumnName(), tableName, cacheMySQLTable.getSensorIdColumnName()))
                        .executeUpdate();
                assert i == 0;
                List<Sensor> sensors = myProperties.cacheMySQLTableSensorMap.get(cacheMySQLTable);
                assert sensors != null;

                for (Sensor sensor : sensors) {
                    Map<Granularity, BitSet> granularityBitSetMap = sensor.bitmap.granularityBitSetMap;
                    for (Granularity granularity : granularityBitSetMap.keySet()) {
                        assert granularityBitSetMap.get(granularity) != null;
                        Blob blob = connection.createBlob();
                        assert blob.setBytes(1, granularityBitSetMap.get(granularity).toByteArray()) == granularityBitSetMap.get(granularity).toByteArray().length;
                        PreparedStatement preparedStatement = connection.prepareStatement(String.format("replace into %s (%s,granularity,bitmapStartDate,bitmapEndDate,bitset) values(?,?,?,?,?)", tableName, cacheMySQLTable.getSensorIdColumnName()));
                        preparedStatement.setString(1, sensor.sensorId);
                        preparedStatement.setString(2, granularity.getAsDisplayString());
                        preparedStatement.setLong(3, sensor.bitmap.startDate.getTime());
                        preparedStatement.setLong(4, sensor.bitmap.endDate.getTime());
                        preparedStatement.setBlob(5, blob);
                        assert preparedStatement.executeUpdate() >= 1;
                        preparedStatement.close();
                    }
                }
                connection.close();
            } catch (SQLException e) {
                LogManager.logError("[" + this.getClass() + "][SaveBitmap]" + e.getMessage());
            }
        }
        Bitmap.isDirty = false;
    }

    public List<Query> fetchQueriesToCheck(Timestamp lastFetchTime) {
        try {
            LogManager.logInfo("[Fetching Queries][cachedResultTill:" + new Date(lastFetchTime.getTime()) + "]");
            List<Query> totalQueries = new ArrayList<>();
            this.cachedResultTill.setTime(System.currentTimeMillis());
            for (MySQLTable cacheLogTable : myProperties.cacheMySQLLogTable) {
                Dataset<Row> generalLog = sparkSession.read().jdbc(cacheLogTable.getURL(), cacheLogTable.getTableName(), cacheLogTable.getProperties());
                List<Query> queries = generalLog
                        .where("event_time < '" + this.cachedResultTill + "' and event_time >= '" + lastFetchTime + "' and command_type='Query' and argument like '%meta_data%' and argument not like '%general_log%'")
                        .select(col("user_host"), col("event_time"), regexp_replace(regexp_replace(col("argument").cast("String"), "\n", " "), "  *", " ").as("argument"))
                        .map(Query.queryMapFunction, Encoders.bean(Query.class))
                        .collectAsList();
                totalQueries.addAll(Query.preprocessQueries(queries));
            }
            return totalQueries;
        } catch (Exception e) {
            this.cachedResultTill.setTime(lastFetchTime.getTime());
            LogManager.logError("[" + this.getClass() + "][Fetching queries]" + e.getMessage());
        }
        return new ArrayList<>();
    }

    public List<Query> fetchQueriesToCheck() {
        return fetchQueriesToCheck((Timestamp) cachedResultTill.clone());
    }

    public void handleQuery(Query query) {
        LogManager.logInfo("[Handling Query][" + query.getQueryStr() + "]");
        Map<Sensor, List<Query.TimeRange>> sensorTimeRangeMap = query.getSensorTimeRangeMap();
        for (Sensor sensor : sensorTimeRangeMap.keySet()) {
            List<Query.TimeRange> timeRanges = sensorTimeRangeMap.get(sensor);
            Granularity granularity = Granularity.eligibleGranularity(timeRanges);
            LogManager.logInfo("[Eligible granularity:" + granularity.getAsDisplayString() + "]");
            ArrayList<Query.TimeRange> nonExistingDataRanges = new ArrayList<>();
            for (Query.TimeRange timeRange : timeRanges) {
                timeRange.startTime = timeRange.startTime - (timeRange.startTime % granularity.getGranularityInTermsOfSeconds());
                timeRange.endTime = timeRange.endTime + (granularity.getGranularityInTermsOfSeconds() - (timeRange.endTime % granularity.getGranularityInTermsOfSeconds()));
                nonExistingDataRanges.addAll(sensor.bitmap.getNonExistingDataRange(granularity, timeRange));
            }
            if (nonExistingDataRanges.size() > 0) {
                updateCache(granularity, sensor, nonExistingDataRanges);
                sensor.bitmap.updateBitmap(granularity, nonExistingDataRanges);
            } else {
                LogManager.logInfo("[Complete data exists of this query]");
            }
        }
    }

    private void updateCache(Granularity granularity, Sensor sensor, ArrayList<Query.TimeRange> nonExistingDataRanges) {
        LogManager.logInfo("[Updating cache databases][NonExistingDataRanges:" + nonExistingDataRanges + "]");
        int interval = granularity.getFetchIntervalAtOnceInSeconds(); // how much data to fetch in each iteration
        int numPartitions = granularity.getNumPartitionsForEachInterval();// This depends on amount of memory a executor has
        MySQLTable sourceMySQLTable = sensor.sourceMySQLTable;
        MySQLTable cacheMySQLTable = sensor.cacheMySQLTable;

        for (Query.TimeRange timeRange : nonExistingDataRanges) {
            LogManager.logInfo("--[Aggregation Started]: " + timeRange);
            timeRange.startTime = timeRange.startTime - (timeRange.startTime % granularity.getGranularityInTermsOfSeconds());
            timeRange.endTime = timeRange.endTime + (granularity.getGranularityInTermsOfSeconds() - (timeRange.endTime % granularity.getGranularityInTermsOfSeconds()));
            long i = timeRange.startTime;
            while (i < timeRange.endTime) {
                long startTime = i;
                long endTime = Math.min(startTime + interval, timeRange.endTime);

                LogManager.logInfo("----[" + new Date(startTime * 1000) + "] to [" + new Date(endTime * 1000) + "]");

                Dataset<Row> sourceMySQLDataset = this.sparkSession.read()
                        .jdbc(sourceMySQLTable.getURL(), sourceMySQLTable.getTableName(), sourceMySQLTable.getTsColumnName(), startTime, endTime, numPartitions, sourceMySQLTable.getProperties());

                sourceMySQLDataset = sourceMySQLDataset.filter(col(sourceMySQLTable.getSensorIdColumnName()).equalTo(sensor.sensorId));

                Column tsFilter = col(sourceMySQLTable.getTsColumnName()).$greater$eq(startTime).and(col(sourceMySQLTable.getTsColumnName()).$less(endTime));

                Dataset<Row> aggregatedDataset = aggregate(sourceMySQLDataset.filter(tsFilter), granularity, sensor);
                assert aggregatedDataset != null;

                aggregatedDataset.write().mode(SaveMode.Append).jdbc(cacheMySQLTable.getURL(), cacheMySQLTable.getTableName(), cacheMySQLTable.getProperties());

                i = endTime;
            }
            LogManager.logInfo("--[Aggregation Finished]: " + timeRange);
        }
    }

    private Dataset<Row> aggregate(Dataset<Row> rows, Granularity granularity, Sensor sensor) {
        Column window = window(col(sensor.sourceMySQLTable.getTsColumnName()).cast(DataTypes.TimestampType), granularity.getName());
        Column count_agg_rows = count("*").as("count_agg_rows");
        Dataset<Row> agg = null;
        if (sensor.cacheMySQLTable.getSchemaType() == MySQLTable.SchemaType.power) {
            Column power = sum("power").as("sum_power");
            Column voltage = sum("voltage").as("sum_voltage");
            Column current = avg("current").as("sum_current");
            Column energyConsumed = last("energy_consumed").as("energy_consumed");
            agg = rows.groupBy(window, col(sensor.sourceMySQLTable.getSensorIdColumnName()).as(sensor.cacheMySQLTable.getSensorIdColumnName()))
                    .agg(power, voltage, current, energyConsumed, count_agg_rows);
        } else if (sensor.cacheMySQLTable.getSchemaType() == MySQLTable.SchemaType.temperature) {
            Column temperature = sum("temperature").as("sum_temperature");
            agg = rows.groupBy(window, col(sensor.sourceMySQLTable.getSensorIdColumnName()).as(sensor.cacheMySQLTable.getSensorIdColumnName()))
                    .agg(temperature, count_agg_rows);
        }
        assert agg != null;
        agg = agg.withColumn(sensor.cacheMySQLTable.getTsColumnName(), col("window.start").cast(DataTypes.DoubleType)).drop("window")
                .withColumn("granularity", lit(granularity.getAsDisplayString()));
        return agg;

    }

    private void startQueryLogging() {
        LogManager.logInfo("[Starting cache databases query logging]");
        for (MySQLTable cacheMySQLLogTable : myProperties.cacheMySQLLogTable) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        LogManager.logInfo("[Query log cleanup][" + cacheMySQLLogTable.getHost() + "]");
                        try {
                            Connection connection = DriverManager.getConnection(cacheMySQLLogTable.getURL(), cacheMySQLLogTable.getProperties());
                            assert connection.prepareStatement("START TRANSACTION").executeUpdate() == 0;
                            assert connection.prepareStatement("SET GLOBAL general_log = 'OFF'").executeUpdate() == 0;
                            assert connection.prepareStatement("RENAME TABLE general_log TO general_log_temp").executeUpdate() == 0;
                            assert connection.prepareStatement("DELETE from mysql.general_log_temp where event_time<'" + cachedResultTill + "'").executeUpdate() >= 0;
                            assert connection.prepareStatement("RENAME TABLE general_log_temp TO general_log").executeUpdate() == 0;
                            assert connection.prepareStatement("SET GLOBAL general_log = 'ON'").executeUpdate() == 0;
                            assert connection.prepareStatement("SET GLOBAL log_output = 'TABLE'").executeUpdate() == 0;
                            assert connection.prepareStatement("SET GLOBAL max_allowed_packet = 1073741824").executeUpdate() == 0;
                            assert connection.prepareStatement("COMMIT").executeUpdate() == 0;
                            connection.close();
                        } catch (SQLException e) {
                            LogManager.logError("[" + this.getClass() + "][startQueryLoggingThread][" + cacheMySQLLogTable + "]" + e.getMessage());
                        }
                        try {
                            Thread.sleep(5 * 60 * 1000);
                        } catch (InterruptedException e) {
                            LogManager.logError("[" + this.getClass() + "][startQueryLoggingThread][" + cacheMySQLLogTable + "]" + e.getMessage());
                        }
                    }
                }
            }).start();
        }
    }

    public void start() {
        LogManager.logInfo("[Cache System Initializing]");
        //Initialization:
        startQueryLogging();
        loadBitmaps();
//        saveGranularities();
        LogManager.logInfo("[Cache System Started]");
        while (true) {
            List<Query> queries = this.fetchQueriesToCheck();
            for (final Query query : queries) {
                handleQuery(query);
            }
            LogManager.logInfo(String.format("[Queries handled:%d]", queries.size()));
            if (Bitmap.isDirty) saveBitmaps();
            try {
                Thread.sleep(2 * 1000);
            } catch (InterruptedException e) {
                LogManager.logError("[" + this.getClass() + "]" + e.getMessage());
            }
        }
    }
}
