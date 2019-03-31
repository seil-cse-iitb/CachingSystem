package manager;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class CacheSystem {
    private final SparkSession sparkSession;
    private final ConfigManager config;
    private final MySQL cacheMySQL;
    private final MySQL mainMySQL;
    private Timestamp cachedResultTill;
    private HashMap<String, Bitmap> bitmaps = new HashMap<>();
    private ArrayList<String> sensorIds = new ArrayList<String>();

    public CacheSystem(ConfigManager config, SparkSession sparkSession) {
        //Testing
        sensorIds.add("power_k_m");
        sensorIds.add("power_k_a");
        sensorIds.add("power_k_seil_p");

        this.config = config;
        this.sparkSession = sparkSession;
        cacheMySQL = new MySQL(config.cacheMySQLHost(), config.cacheMySQLUser(), config.cacheMySQLPassword(), config.cacheMySQLDatabase());
        mainMySQL = new MySQL(config.mainMySQLHost(), config.mainMySQLUser(), config.mainMySQLPassword(), config.mainMySQLDatabase());
//        cachedResultTill = new Timestamp(0); //comment it after testing
        cachedResultTill = new Timestamp(System.currentTimeMillis()); //uncomment once testing is done
        bitmaps.put("1 minute", new Bitmap(sensorIds, "1 minute"));
        bitmaps.put("1 hour", new Bitmap(sensorIds, "1 hour"));
        bitmaps.put("1 day", new Bitmap(sensorIds, "1 day"));
    }

    public static void log(String msg) {
        System.out.println(msg);
    }

    public List<Query> fetchQueriesToCheck(Timestamp timestamp) {
        log("Fetching Queries after time:" + new Date(timestamp.getTime()) + " and cachedResultTill:" + new Date(cachedResultTill.getTime()));
        cachedResultTill.setTime(System.currentTimeMillis());
        Dataset<Row> general_log = sparkSession.read().jdbc(cacheMySQL.getURL("mysql"), "general_log", cacheMySQL.getProperties());
        List<Query> queries = general_log
                .where("event_time>='" + timestamp + "' and command_type='Query' and argument like '%meta_data%' and argument not like '%general_log%'")
                .select(col("user_host"), col("event_time"), regexp_replace(regexp_replace(col("argument").cast("String"), "\n", " "), "  *", " ").as("argument"))
                .map(Query.queryMapFunction, Encoders.bean(Query.class))
                .collectAsList();
        Query.preprocessQueries(queries);
        return queries;
    }

    public List<Query> fetchQueriesToCheck() {
        return fetchQueriesToCheck((Timestamp) cachedResultTill.clone());
    }

    public void handleQuery(Query query) {
        log("\n\n\n-----------------------------------");
        log("Query:" + query);
        log("Granularity:" + query.granularity);

        Bitmap bitmap = bitmaps.get(query.granularity);
        ArrayList<String> queriedSensorIds = query.getSensorIds();
        ArrayList<Query.TimeRange> timeRanges = query.getTimeRanges();
        log("Query Time ranges:" + timeRanges);
        for (String sensorId : queriedSensorIds) {
            ArrayList<Query.TimeRange> nonExistingDataRanges = new ArrayList<>();
            try {
                for (Query.TimeRange timeRange : timeRanges) {
                    nonExistingDataRanges.addAll(bitmap.getNonExistingDataRange(sensorId, timeRange));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            log("NonExistingDataRange:" + nonExistingDataRanges);
            this.updateCache(query, sensorId, nonExistingDataRanges);
            bitmap.updateBitmap(query, sensorId, nonExistingDataRanges);
        }
    }

    private void updateCache(Query query, String sensorId, ArrayList<Query.TimeRange> nonExistingDataRanges) {
        log("\nNumber of nonExistingDataRanges: " + nonExistingDataRanges.size());
        Integer interval; // how much data to fetch in each iteration
        int numPartitions;// This depends on amount of memory a executor has
        if (query.granularity.equalsIgnoreCase("1 minute")) {
            interval = 5 * 60 * 60; // 5 hours
            numPartitions = 10; // 30 minutes per partition
        } else if (query.granularity.equalsIgnoreCase("1 hour")) {
            interval = 2 * 24 * 60 * 60; // 2 days
            numPartitions = 48; //1 hours per partition
        } else {
            interval = 10 * 24 * 60 * 60; // 10 days
            numPartitions = 20; //12 hours per partition
        }
        log("Interval: " + interval / 60 / 60 + " hours");
        log("NumPartitions: " + numPartitions);
        for (Query.TimeRange timeRange : nonExistingDataRanges) {
            log("Aggregation Started: " + timeRange);
            long i = timeRange.startTime;
            while (i < timeRange.endTime) {
                long startTime = i;
                long endTime = Math.min(startTime + interval, timeRange.endTime);
                log("--[" + new Date(startTime*1000) + "] to [" + new Date(endTime*1000) + "]");
                Dataset<Row> mainMySQLDataset = this.sparkSession.read()
                        .jdbc(mainMySQL.getURL(), query.tableName, query.tsColumnName, startTime, endTime, numPartitions, mainMySQL.getProperties());
                mainMySQLDataset = mainMySQLDataset.filter(col(query.sensorIdColumnName).equalTo(sensorId));
                Column tsFilter = col(query.tsColumnName).$greater$eq(startTime).and(col(query.tsColumnName).$less(endTime));
                Dataset<Row> aggregatedDataset = aggregateSch3(mainMySQLDataset.filter(tsFilter), query);
                aggregatedDataset.write().mode(SaveMode.Append).jdbc(cacheMySQL.getURL(), query.tableName, cacheMySQL.getProperties());
                i = endTime;
            }
            log("Aggregation Finished: " + timeRange);
            log("\n\n\n");
        }
    }

    private Dataset<Row> aggregateSch3(Dataset<Row> rows, Query query) {
        Column window = window(col(query.tsColumnName).cast(DataTypes.TimestampType), query.granularity);
        Column W = avg("W").as("W");
        Column W1 = avg("W1").as("W1");
        Column W2 = avg("W2").as("W2");
        Column W3 = avg("W3").as("W3");
        Column VA = avg("VA").as("VA");
        Column VA1 = avg("VA1").as("VA1");
        Column VA2 = avg("VA2").as("VA2");
        Column VA3 = avg("VA3").as("VA3");

        Dataset<Row> agg = rows.groupBy(window, col(query.sensorIdColumnName))
                .agg(
                        first("srl").as("srl"),
                        avg("F").as("F"),
                        W, W1, W2, W3,
//                functions.avg("V").as("V"),
                        avg("V1").as("V1"),
                        avg("V2").as("V2"),
                        avg("V3").as("V3"),
                        avg("A").as("A"),
                        avg("A1").as("A1"),
                        avg("A2").as("A2"),
                        avg("A3").as("A3"),
                        avg("VAR").as("VAR"),
                        avg("VAR1").as("VAR1"),
                        avg("VAR2").as("VAR2"),
                        avg("VAR3").as("VAR3"),
                        VA, VA1, VA2, VA3,
                        W.divide(VA).as("PF"),
                        W1.divide(VA1).as("PF1"),
                        W2.divide(VA2).as("PF2"),
                        W3.divide(VA3).as("PF3"),
                        last("TS_RECV").as("TS_RECV"),
                        last("FwdVAh").as("FwdVAh"),
                        last("FwdVARhC").as("FwdVARhC"),
                        last("FwdVARhR").as("FwdVARhR"),
                        last("FwdWh").as("FwdWh")
//                        last("FwdWh").minus(first("FwdWh")).as("delta_FwdWh"),
//                        count("srl").as("count_agg_rows") //counts no. of rows aggregated
                )
                .withColumn(query.tsColumnName, col("window.start").cast(DataTypes.DoubleType))
                .drop("window")
                .withColumn("granularity", lit(query.granularity));
//                .select(col("W"),col("W1"),col("W2"),col("W3"),col("VA"),col("VA1"),col("VA2"),col("VA3"),col("srl"),col("F"),col("V1"),col("V2"),col("V3"),col("A"),col("A1"),col("A2"),col("A3"),col("VAR"),col("VAR1"),col("VAR2"),col("VAR3"),col("TS_RECV"),col("FwdVAh"),col("FwdVARhC"),col("FwdVARhR"),col("FwdWh"))
//                .orderBy(query.tsColumnName);
        return agg;
    }

    public void start() {
        while (true) {
            List<Query> queries = this.fetchQueriesToCheck();
            for (final Query query : queries) {
                handleQuery(query);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
