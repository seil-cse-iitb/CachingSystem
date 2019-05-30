package version1.main;

import version1.manager.CacheSystem;
import version1.manager.LogManager;
import version1.manager.PropertiesManager;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

//spark-submit --class version1.main.Main --master spark://10.129.149.70:7077 --deploy-mode client target/CachingSystem-1.0-SNAPSHOT-jar-with-dependencies.jar ./cachesystem.properties
public class Main {

    public static SparkSession sparkSession = null;

    public static void main(String[] args) {
        if (args.length > 0) {
            String propertiesFileName = args[0];
            PropertiesManager.loadProperties(propertiesFileName);
        }
        LogManager.logDebugInfo("[MyProperties]"+PropertiesManager.getProperties());
        if (PropertiesManager.getProperties().STOP_SPARK_LOGGING)
            logsOff();
        SparkConf conf = new SparkConf().setAppName(PropertiesManager.getProperties().APP_NAME);
        conf.setIfMissing("spark.master", "local[4]");
        sparkSession = SparkSession.builder().config(conf).getOrCreate();
        CacheSystem cacheSystem = new CacheSystem(sparkSession);
        cacheSystem.start();
    }

    public static void logsOff() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }
}


//SELECT * FROM `general_log` where command_type='Query' and argument like '%meta_data%' and argument not like '%general_log%'
//SELECT FLOOR(TS)     as             time_sec, W     as             value     , "Power Sockets"         as         metric,         "from_cache"         as         meta_data         FROM         sch_3         WHERE         TS >=         1552321566         AND         TS <=         1552343166         and         sensor_id =         'power_k_seil_p'         ORDER         BY         TS         ASC