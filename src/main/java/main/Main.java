package main;

import beans.ConfigurationBean;
import controllers.CacheSystemController;
import controllers.ConfigurationController;
import managers.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import static managers.Utils.sparkLogsOff;

public class Main {
    public static void main(String[] args) {
        if (args.length <= 0) {
            System.out.println("Configuration file path is needed as command line argument!!");
            return;
        }
        ConfigurationController configController = new ConfigurationController();
        String configurationFilePath= args[0];
        ConfigurationBean cb = configController.readConfiguration(configurationFilePath);

        LogManager.logDebugInfo("[Configuration]"+cb.getJsonObject());
        if (cb.stopSparkLogging)
            sparkLogsOff();
        SparkConf conf = new SparkConf().setAppName(cb.appName);
        conf.setIfMissing("spark.master", "local[4]");
        conf.set("spark.scheduler.mode", "FAIR");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        CacheSystemController cacheSystemController = new CacheSystemController(sparkSession,cb);
        cacheSystemController.start();
    }
}
