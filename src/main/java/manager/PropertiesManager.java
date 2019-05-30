package manager;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PropertiesManager {
    private final static MyProperties myProperties = new MyProperties();
    private static DateFormat df = new SimpleDateFormat("dd-MM-yyyy");

    public static MyProperties loadProperties(String propFileName) {
        try {
            final Properties properties = new Properties();
            properties.load(new FileInputStream(propFileName));

            //Other Properties
            myProperties.REPORT_RECEIVER_EMAIL = properties.getProperty("reportReceiverEmail");
            myProperties.LOG_FILE_PATH = properties.getProperty("logFilePath");
            myProperties.APP_NAME = properties.getProperty("appName");
            myProperties.REPORT_ERROR = Boolean.parseBoolean(properties.getProperty("reportError"));
            myProperties.DEBUG = Boolean.parseBoolean(properties.getProperty("debug"));
            myProperties.STOP_SPARK_LOGGING = Boolean.parseBoolean(properties.getProperty("stopSparkLogging"));


            //Bitmap properties
            String bitmapStartDate = properties.getProperty("bitmap.startDate");
            String bitmapEndDate = properties.getProperty("bitmap.endDate");
            //Granularity properties
            int granularityCount = Integer.parseInt(properties.getProperty("bitmap.granularityCount"));
            for (int i = 1; i <= granularityCount; i++) {
                String granularityName = properties.getProperty("bitmap.granularity" + i + ".name");
                int granularityInTermsOfSeconds = Integer.parseInt(properties.getProperty("bitmap.granularity" + i + ".inTermsOfSeconds"));
                int displayLimitInSeconds = Integer.parseInt(properties.getProperty("bitmap.granularity" + i + ".displayLimitInSeconds"));
                int displayPriority = Integer.parseInt(properties.getProperty("bitmap.granularity" + i + ".displayPriority"));
                int fetchIntervalAtOnceInSeconds = Integer.parseInt(properties.getProperty("bitmap.granularity" + i + ".fetchIntervalAtOnceInSeconds"));
                int numPartitionsForEachInterval = Integer.parseInt(properties.getProperty("bitmap.granularity" + i + ".numPartitionsForEachInterval"));
                Granularity granularity = new Granularity(granularityName, granularityInTermsOfSeconds, displayLimitInSeconds, displayPriority, fetchIntervalAtOnceInSeconds, numPartitionsForEachInterval);
                myProperties.granularities.add(granularity);
            }

            //Cache MySQL Tables Properties
            int cacheMySQLCount = Integer.parseInt(properties.getProperty("cacheMySQLCount"));
            for (int i = 1; i <= cacheMySQLCount; i++) {
                String host = properties.getProperty("cacheMySQL" + i + ".host");
                String user = properties.getProperty("cacheMySQL" + i + ".user");
                String password = properties.getProperty("cacheMySQL" + i + ".password");
                String database = properties.getProperty("cacheMySQL" + i + ".database");
                myProperties.cacheMySQLLogTable.add(new MySQLTable(host, user, password, "mysql", "general_log", null, null, null));
                int tableCount = Integer.parseInt(properties.getProperty("cacheMySQL" + i + ".tableCount"));
                for (int j = 1; j <= tableCount; j++) {
                    String tableName = properties.getProperty("cacheMySQL" + i + ".table" + j + ".name");
                    MySQLTable.SchemaType schemaType = MySQLTable.SchemaType.valueOf(properties.getProperty("cacheMySQL" + i + ".table" + j + ".schemaType"));
                    String tsColumnName = properties.getProperty("cacheMySQL" + i + ".table" + j + ".tsColumnName");
                    String sensorIdColumnName = properties.getProperty("cacheMySQL" + i + ".table" + j + ".sensorIdColumnName");
                    MySQLTable cacheMySQLTable = new MySQLTable(host, user, password, database, tableName, schemaType, sensorIdColumnName, tsColumnName);
                    myProperties.cacheMySQLTableSchemaTypeMap.put(schemaType, cacheMySQLTable);
                    myProperties.cacheMySQLTableSensorMap.put(cacheMySQLTable,new ArrayList<>());
//                    myProperties.cacheMySQLTableMap.put(tableName,cacheMySQLTable);
                }
            }

            //Source MySQL Tables Properties
            int sourceMySQLCount = Integer.parseInt(properties.getProperty("sourceMySQLCount"));
            for (int i = 1; i <= sourceMySQLCount; i++) {
                String host = properties.getProperty("sourceMySQL" + i + ".host");
                String user = properties.getProperty("sourceMySQL" + i + ".user");
                String password = properties.getProperty("sourceMySQL" + i + ".password");
                String database = properties.getProperty("sourceMySQL" + i + ".database");
                int tableCount = Integer.parseInt(properties.getProperty("sourceMySQL" + i + ".tableCount"));
                for (int j = 1; j <= tableCount; j++) {
                    String table = properties.getProperty("sourceMySQL" + i + ".table" + j + ".name");
                    MySQLTable.SchemaType schemaType = MySQLTable.SchemaType.valueOf(properties.getProperty("sourceMySQL" + i + ".table" + j + ".schemaType"));
                    String tsColumnName = properties.getProperty("sourceMySQL" + i + ".table" + j + ".tsColumnName");
                    String sensorIdColumnName = properties.getProperty("sourceMySQL" + i + ".table" + j + ".sensorIdColumnName");
                    MySQLTable sourceMySQLTable = new MySQLTable(host, user, password, database, table, schemaType, sensorIdColumnName, tsColumnName);
                    MySQLTable cacheMySQLTable = myProperties.cacheMySQLTableSchemaTypeMap.get(schemaType);
                    // TODO: if sensor count not available then fetch from table using distinct sensorId query
                    int sensorCount = Integer.parseInt(properties.getProperty("sourceMySQL" + i + ".table" + j + ".sensorCount"));
                    for (int k = 1; k <= sensorCount; k++) {
                        String sensorId = properties.getProperty("sourceMySQL" + i + ".table" + j + ".sensor" + k + ".sensorId");
                        Bitmap bitmap = new Bitmap(df.parse(bitmapStartDate), df.parse(bitmapEndDate), myProperties.granularities);
                        Sensor sensor = new Sensor(sensorId, sourceMySQLTable, cacheMySQLTable, bitmap);
                        myProperties.sensorMap.put(sensorId, sensor);
                        myProperties.cacheMySQLTableSensorMap.get(cacheMySQLTable).add(sensor);
                    }
                }
            }

        } catch (IOException | ParseException e) {
            e.printStackTrace();
            LogManager.logError(e.getMessage());
        }
        return myProperties;
    }

    public static MyProperties getProperties() {
        return myProperties;
    }
}
