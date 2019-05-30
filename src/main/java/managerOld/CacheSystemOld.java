package managerOld;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class CacheSystemOld {
    private final SparkSession sparkSession;
    private final ConfigManager config;
    private final MySQLOld cacheMySQLOld;
    private final MySQLOld mainMySQLOld;
    private Timestamp cachedResultTill;
    private HashMap<String, BitmapOld> bitmaps = new HashMap<>();
    private ArrayList<String> sensorIds = new ArrayList<String>();

    public CacheSystemOld(ConfigManager config, SparkSession sparkSession) {
        //Testing
        sensorIds.add("power_k_m");
        sensorIds.add("power_k_a");
        sensorIds.add("power_k_seil_p");

        this.config = config;
        this.sparkSession = sparkSession;
        cacheMySQLOld = new MySQLOld(config.cacheMySQLHost(), config.cacheMySQLUser(), config.cacheMySQLPassword(), config.cacheMySQLDatabase());
        mainMySQLOld = new MySQLOld(config.mainMySQLHost(), config.mainMySQLUser(), config.mainMySQLPassword(), config.mainMySQLDatabase());
//        cachedResultTill = new Timestamp(0); //comment it after testing
        cachedResultTill = new Timestamp(System.currentTimeMillis()); //uncomment once testing is done
        bitmaps.put("1 minute", new BitmapOld(sensorIds, "1 minute"));
        bitmaps.put("1 hour", new BitmapOld(sensorIds, "1 hour"));
        bitmaps.put("1 day", new BitmapOld(sensorIds, "1 day"));
    }

    public static void log(String msg) {
        System.out.println(msg);
    }

    public List<QueryOld> fetchQueriesToCheck(Timestamp timestamp) {
        log("Fetching Queries after time:" + new Date(timestamp.getTime()) + " and cachedResultTill:" + new Date(cachedResultTill.getTime()));
        cachedResultTill.setTime(System.currentTimeMillis());
        Dataset<Row> general_log = sparkSession.read().jdbc(cacheMySQLOld.getURL("mysql"), "general_log", cacheMySQLOld.getProperties());
        List<QueryOld> queries = general_log
                .where("event_time>='" + timestamp + "' and command_type='Query' and argument like '%meta_data%' and argument not like '%general_log%'")
                .select(col("user_host"), col("event_time"), regexp_replace(regexp_replace(col("argument").cast("String"), "\n", " "), "  *", " ").as("argument"))
                .map(QueryOld.queryMapFunction, Encoders.bean(QueryOld.class))
                .collectAsList();
        QueryOld.preprocessQueries(queries);
        return queries;
    }

    public List<QueryOld> fetchQueriesToCheck() {
        return fetchQueriesToCheck((Timestamp) cachedResultTill.clone());
    }

    public void handleQuery(QueryOld queryOld) {
        log("\n\n\n-----------------------------------");
        log("Query:" + queryOld);
        log("Granularity:" + queryOld.granularity);

        BitmapOld bitmapOld = bitmaps.get(queryOld.granularity);
        ArrayList<String> queriedSensorIds = queryOld.getSensorIds();
        ArrayList<QueryOld.TimeRange> timeRanges = queryOld.getTimeRanges();
        log("Query Time ranges:" + timeRanges);
        for (String sensorId : queriedSensorIds) {
            ArrayList<QueryOld.TimeRange> nonExistingDataRanges = new ArrayList<>();
            try {
                for (QueryOld.TimeRange timeRange : timeRanges) {
                    nonExistingDataRanges.addAll(bitmapOld.getNonExistingDataRange(sensorId, timeRange));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            log("NonExistingDataRange:" + nonExistingDataRanges);
            this.updateCache(queryOld, sensorId, nonExistingDataRanges);
            bitmapOld.updateBitmap(queryOld, sensorId, nonExistingDataRanges);
        }
    }

    private void updateCache(QueryOld queryOld, String sensorId, ArrayList<QueryOld.TimeRange> nonExistingDataRanges) {
        log("\nNumber of nonExistingDataRanges: " + nonExistingDataRanges.size());
        Integer interval; // how much data to fetch in each iteration
        int numPartitions;// This depends on amount of memory a executor has
        if (queryOld.granularity.equalsIgnoreCase("1 minute")) {
            interval = 5 * 60 * 60; // 5 hours
            numPartitions = 10; // 30 minutes per partition
        } else if (queryOld.granularity.equalsIgnoreCase("1 hour")) {
            interval = 2 * 24 * 60 * 60; // 2 days
            numPartitions = 48; //1 hours per partition
        } else {
            interval = 10 * 24 * 60 * 60; // 10 days
            numPartitions = 20; //12 hours per partition
        }
        log("Interval: " + interval / 60 / 60 + " hours");
        log("NumPartitions: " + numPartitions);
        for (QueryOld.TimeRange timeRange : nonExistingDataRanges) {
            log("Aggregation Started: " + timeRange);
            long i = timeRange.startTime;
            while (i < timeRange.endTime) {
                long startTime = i;
                long endTime = Math.min(startTime + interval, timeRange.endTime);
                log("--[" + new Date(startTime*1000) + "] to [" + new Date(endTime*1000) + "]");
                Dataset<Row> mainMySQLDataset = this.sparkSession.read()
                        .jdbc(mainMySQLOld.getURL(), queryOld.tableName, queryOld.tsColumnName, startTime, endTime, numPartitions, mainMySQLOld.getProperties());
                mainMySQLDataset = mainMySQLDataset.filter(col(queryOld.sensorIdColumnName).equalTo(sensorId));
                Column tsFilter = col(queryOld.tsColumnName).$greater$eq(startTime).and(col(queryOld.tsColumnName).$less(endTime));
                Dataset<Row> aggregatedDataset = aggregateSch3(mainMySQLDataset.filter(tsFilter), queryOld);
                aggregatedDataset.write().mode(SaveMode.Append).jdbc(cacheMySQLOld.getURL(), queryOld.tableName, cacheMySQLOld.getProperties());
                i = endTime;
            }
            log("Aggregation Finished: " + timeRange);
            log("\n\n\n");
        }
    }

    private Dataset<Row> aggregateSch3(Dataset<Row> rows, QueryOld queryOld) {
        Column window = window(col(queryOld.tsColumnName).cast(DataTypes.TimestampType), queryOld.granularity);
        Column W = avg("W").as("W");
        Column W1 = avg("W1").as("W1");
        Column W2 = avg("W2").as("W2");
        Column W3 = avg("W3").as("W3");
        Column VA = avg("VA").as("VA");
        Column VA1 = avg("VA1").as("VA1");
        Column VA2 = avg("VA2").as("VA2");
        Column VA3 = avg("VA3").as("VA3");

        Dataset<Row> agg = rows.groupBy(window, col(queryOld.sensorIdColumnName))
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
                .withColumn(queryOld.tsColumnName, col("window.start").cast(DataTypes.DoubleType))
                .drop("window")
                .withColumn("granularity", lit(queryOld.granularity));
//                .select(col("W"),col("W1"),col("W2"),col("W3"),col("VA"),col("VA1"),col("VA2"),col("VA3"),col("srl"),col("F"),col("V1"),col("V2"),col("V3"),col("A"),col("A1"),col("A2"),col("A3"),col("VAR"),col("VAR1"),col("VAR2"),col("VAR3"),col("TS_RECV"),col("FwdVAh"),col("FwdVARhC"),col("FwdVARhR"),col("FwdWh"))
//                .orderBy(query.tsColumnName);
        return agg;
    }

    public void start() {
        while (true) {
            List<QueryOld> queries = this.fetchQueriesToCheck();
            for (final QueryOld queryOld : queries) {
                handleQuery(queryOld);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
