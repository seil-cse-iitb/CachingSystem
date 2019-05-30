package manager;

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
                ", REPORT_RECEIVER_EMAIL='" + REPORT_RECEIVER_EMAIL + '\'' +
                ", LOG_FILE_PATH='" + LOG_FILE_PATH + '\'' +
                ", APP_NAME='" + APP_NAME + '\'' +
                ", REPORT_ERROR=" + REPORT_ERROR +
                ", STOP_SPARK_LOGGING=" + STOP_SPARK_LOGGING +
                ", GRANULARITY_TABLE_NAME_SUFFIX='" + GRANULARITY_TABLE_NAME_SUFFIX + '\'' +
                ", BITMAP_TABLE_NAME_SUFFIX='" + BITMAP_TABLE_NAME_SUFFIX + '\'' +
                '}';
    }
}
