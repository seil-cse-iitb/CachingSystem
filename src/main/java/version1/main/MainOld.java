package version1.main;

import version1.managerOld.CacheSystemOld;
import version1.managerOld.ConfigManager;
import org.aeonbits.owner.ConfigFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;

import static version1.managerOld.CacheSystemOld.log;

//spark-submit --class version1.main.Main --master spark://10.129.149.70:7077 --deploy-mode client target/CachingSystem-1.0-SNAPSHOT-jar-with-dependencies.jar ./cachesystemold.properties
public class MainOld {

    public static DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    public static ConfigManager config = null;
    public static SparkSession sparkSession = null;

    public static void main(String[] args) {
        logsOff();
        config = loadConfig(args);
        System.out.println("[Config]: " + config);
        SparkConf conf = new SparkConf().setAppName(config.appName()).set("spark.driver.host", config.driverHostname());
        conf.setIfMissing("spark.master", "local[4]");
        sparkSession = SparkSession.builder().config(conf).getOrCreate();
        log(sparkSession.conf().get("spark.executor.memory"));

        CacheSystemOld cacheSystemOld = new CacheSystemOld(config, sparkSession);
        cacheSystemOld.start();
    }
    public static void logsOff() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }

    private static ConfigManager loadConfig(String[] args) {
        ConfigManager cfg;
        if (args.length > 0) {
            try {
                Properties props = new Properties();
                props.load(new FileInputStream(new File(args[0])));
                cfg = ConfigFactory.create(ConfigManager.class, props);
                return cfg;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        cfg = ConfigFactory.create(ConfigManager.class);
        return cfg;
    }
}


//SELECT * FROM `general_log` where command_type='Query' and argument like '%meta_data%' and argument not like '%general_log%'
//SELECT FLOOR(TS)     as             time_sec, W     as             value     , "Power Sockets"         as         metric,         "from_cache"         as         meta_data         FROM         sch_3         WHERE         TS >=         1552321566         AND         TS <=         1552343166         and         sensor_id =         'power_k_seil_p'         ORDER         BY         TS         ASC