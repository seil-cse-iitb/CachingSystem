package main;

import beans.ConfigurationBean;
import beans.QueryBean;
import controllers.CacheSystemController;
import controllers.ConfigurationController;
import managers.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static managers.Utils.sparkLogsOff;

public class Main {
    public static boolean runPredefinedQueries=false;
    public static boolean serialQueryExecution = false;
    public static boolean singleTimeExecution = false;
    public static boolean clearCacheSystemInitially = false;
    public static boolean clearCacheSystemAfterEveryQuery=false;
    public static boolean logExecutionStatistics=false;
    private static String queryType = "zoomOutQueries";//zoomInQueries,zoomOutQueries,slideQueries

    public static void main(String[] args) {
        if (args.length <= 0) {
            System.out.println("Configuration file path is needed as command line argument!!");
            return;
        }
        ConfigurationController configController = new ConfigurationController();
        String configurationFilePath = args[0];
        ConfigurationBean cb = configController.readConfiguration(configurationFilePath);

        LogManager.logDebugInfo("[Configuration]" + cb.getJsonObject());
        if (cb.stopSparkLogging)
            sparkLogsOff();
        SparkConf conf = new SparkConf().setAppName(cb.appName);
        conf.setIfMissing("spark.master", "local[4]");
        conf.set("spark.scheduler.mode", "FAIR");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        CacheSystemController cacheSystemController = new CacheSystemController(sparkSession, cb);
        cacheSystemController.start();
    }
//    SELECT FLOOR(TS) as time_sec, power as value,'power_sensor' as metric, 'from_cache' as meta_data FROM power WHERE   TS >= 1514745000 AND TS <= 1514831400 and sensor_id ='power_k_seil_p' and granularity like '1 hour' order by time_sec

    public static List<QueryBean> getNewQueries() {
        ArrayList<String> zoomInQueries = new ArrayList<>();
        zoomInQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1451586600 AND TS <= 1540060200 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomInQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1527445800 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomInQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1505327400 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomInQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1494268200 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomInQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1488738600 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomInQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1485973800 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomInQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1484591400 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomInQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1483900200 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomInQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1483554600 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomInQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1483381800 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomInQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1483295400 and sensor_id ='power_k_seil_p' order by time_sec");

        ArrayList<String> zoomOutQueries = new ArrayList<>();
        zoomOutQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1483295400 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomOutQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1483381800 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomOutQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1483554600 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomOutQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1483900200 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomOutQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1484591400 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomOutQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1485973800 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomOutQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1488738600 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomOutQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1494268200 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomOutQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1505327400 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomOutQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1527445800 and sensor_id ='power_k_seil_p' order by time_sec");
        zoomOutQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1451586600 AND TS <= 1540060200 and sensor_id ='power_k_seil_p' order by time_sec");

        ArrayList<String> slideQueries = new ArrayList<>();//correct queries
        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1514745000 and sensor_id ='power_k_seil_p' order by time_sec");
        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1498933800 AND TS <= 1530469800 and sensor_id ='power_k_seil_p' order by time_sec");
        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1467397800 AND TS <= 1498933800 and sensor_id ='power_k_seil_p' order by time_sec");
        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1519842600 AND TS <= 1522434600 and sensor_id ='power_k_seil_p' order by time_sec");
        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1521138600 AND TS <= 1523730600 and sensor_id ='power_k_seil_p' order by time_sec");
        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1518546600 AND TS <= 1521138600 and sensor_id ='power_k_seil_p' order by time_sec");
        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1527877800 AND TS <= 1527964200 and sensor_id ='power_k_seil_p' order by time_sec");
        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1527921000 AND TS <= 1528007400 and sensor_id ='power_k_seil_p' order by time_sec");
        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1527834600 AND TS <= 1527921000 and sensor_id ='power_k_seil_p' order by time_sec");
        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1533079800 AND TS <= 1533097800 and sensor_id ='power_k_seil_p' order by time_sec");
        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1533088800 AND TS <= 1533106800 and sensor_id ='power_k_seil_p' order by time_sec");
        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1533070800 AND TS <= 1533088800 and sensor_id ='power_k_seil_p' order by time_sec");

        ArrayList<String> adhocQueries = new ArrayList<>();
        adhocQueries.add("SELECT FLOOR(TS) as time_sec, power as value, 'power_k_a' as metric, 'from_cache' as ignore_cache FROM power WHERE TS >= 1546281000 AND TS <= 1546453800 and sensor_id ='power_k_a' and granularity like '%' order by time_sec");


        ArrayList<String> queryStrs = zoomOutQueries;

        if (queryType.equalsIgnoreCase("zoomOutQueries"))
            queryStrs = zoomOutQueries;
        else if (queryType.equalsIgnoreCase("zoomInQueries"))
            queryStrs = zoomInQueries;
        else if (queryType.equalsIgnoreCase("slideQueries"))
            queryStrs = slideQueries;

//        queryStrs = adhocQueries;

        ArrayList<QueryBean> queries = new ArrayList<>();
        QueryBean queryBean;
        for (String queryStr : queryStrs) {
            queryBean = new QueryBean();
            queryBean.setQueryStr(queryStr);
            queryBean.setUserHost("10.129.149.32");
            queryBean.setQueryTimestamp(new Timestamp(System.currentTimeMillis()));
            queries.add(queryBean);
        }
        return queries;
    }
}


//    ArrayList<String> zoomInQueries = new ArrayList<>();
//        zoomInQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1514745000 and sensor_id ='power_k_seil_p' order by time_sec");
//                zoomInQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1496255400 and sensor_id ='power_k_seil_p' order by time_sec");
//                zoomInQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1485887400 and sensor_id ='power_k_seil_p' order by time_sec");
//                zoomInQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1484418600 and sensor_id ='power_k_seil_p' order by time_sec");
//                zoomInQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483209000 AND TS <= 1483295400 and sensor_id ='power_k_seil_p' order by time_sec");
//                zoomInQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483252200 AND TS <= 1483270200 and sensor_id ='power_k_seil_p' order by time_sec");
//
//                ArrayList<String> zoomOutQueries = new ArrayList<>();
//        zoomOutQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1514788200 AND TS <= 1514806200 and sensor_id ='power_k_seil_p' order by time_sec");
//        zoomOutQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1514745000 AND TS <= 1514831400 and sensor_id ='power_k_seil_p' order by time_sec");
//        zoomOutQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1514745000 AND TS <= 1515954600 and sensor_id ='power_k_seil_p' order by time_sec");
//        zoomOutQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1514745000 AND TS <= 1517423400 and sensor_id ='power_k_seil_p' order by time_sec");
//        zoomOutQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1514745000 AND TS <= 1527791400 and sensor_id ='power_k_seil_p' order by time_sec");
//        zoomOutQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1514745000 AND TS <= 1546281000 and sensor_id ='power_k_seil_p' order by time_sec");
//
//        ArrayList<String> slideQueries = new ArrayList<>();
//        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1514745000 AND TS <= 1546281000 and sensor_id ='power_k_seil_p' order by time_sec");
//        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1527791400 AND TS <= 1559327400 and sensor_id ='power_k_seil_p' order by time_sec");
//        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1496255400 AND TS <= 1527791400 and sensor_id ='power_k_seil_p' order by time_sec");
//        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1488306600 AND TS <= 1490985000 and sensor_id ='power_k_seil_p' order by time_sec");
//        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1487097000 AND TS <= 1489516200 and sensor_id ='power_k_seil_p' order by time_sec");
//        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1489516200 AND TS <= 1492194600 and sensor_id ='power_k_seil_p' order by time_sec");
//        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1486233000 AND TS <= 1486319400 and sensor_id ='power_k_seil_p' order by time_sec");
//        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1486189800 AND TS <= 1486276200 and sensor_id ='power_k_seil_p' order by time_sec");
//        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1486276200 AND TS <= 1486362600 and sensor_id ='power_k_seil_p' order by time_sec");
//        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483227000 AND TS <= 1483245000 and sensor_id ='power_k_seil_p' order by time_sec");
//        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483236000 AND TS <= 1483254000 and sensor_id ='power_k_seil_p' order by time_sec");
//        slideQueries.add("SELECT FLOOR(TS) as time_sec, power as value,'power_k_seil_p' as metric, 'from_cache' as meta_data FROM power WHERE TS >= 1483218000 AND TS <= 1483236000 and sensor_id ='power_k_seil_p' order by time_sec");
//
//1