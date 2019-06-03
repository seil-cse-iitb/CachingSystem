package version1.manager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyProperties {

    public static boolean DEBUG = false;
    public final Map<String, Sensor> sensorMap = new HashMap<>();
    public final Map<MySQLTable.SchemaType, MySQLTable> cacheMySQLTableSchemaTypeMap = new HashMap<>(); //SchemaType(power,temperature) vs cacheTableObject
    //    public final Map<String, MySQLTable> cacheMySQLTableMap = new HashMap<>();//tableName vs cacheTableObject
    public final List<MySQLTable> cacheMySQLLogTable = new ArrayList<>();
    public final List<Granularity> granularities = new ArrayList<>();
    public final Map<MySQLTable, List<Sensor>> cacheMySQLTableSensorMap = new HashMap<>();

    public String REPORT_RECEIVER_EMAIL = "sapantanted99@gmail.com";
    public String LOG_FILE_PATH = "/tmp/cache-system.log";
    public String APP_NAME = "CacheSystem";
    public boolean REPORT_ERROR = true;
    public boolean STOP_SPARK_LOGGING = false;
    public String GRANULARITY_TABLE_NAME_SUFFIX = "granularity";
    public String BITMAP_TABLE_NAME_SUFFIX = "bitmap";


    @Override
    public String toString() {
        return "MyProperties{" +
                "sensorMap=" + sensorMap +
                ", cacheMySQLTableSchemaTypeMap=" + cacheMySQLTableSchemaTypeMap +
                ", cacheMySQLLogTable=" + cacheMySQLLogTable +
                ", granularities=" + granularities +
                ", cacheMySQLTableSensorMap=" + cacheMySQLTableSensorMap +
                ", reportReceiverEmail='" + REPORT_RECEIVER_EMAIL + '\'' +
                ", logFilePath='" + LOG_FILE_PATH + '\'' +
                ", appName='" + APP_NAME + '\'' +
                ", reportError=" + REPORT_ERROR +
                ", stopSparkLogging=" + STOP_SPARK_LOGGING +
                ", granularityTableNameSuffix='" + GRANULARITY_TABLE_NAME_SUFFIX + '\'' +
                ", bitmapTableNameSuffix='" + BITMAP_TABLE_NAME_SUFFIX + '\'' +
                '}';
    }
}
