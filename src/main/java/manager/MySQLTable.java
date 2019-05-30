package manager;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Properties;

public class MySQLTable {
    public final static StructType powerTableSchema = new StructType(new StructField[]{
            new StructField("granularity", DataTypes.IntegerType, true, null),
            new StructField("sensor_id", DataTypes.StringType, true, null),
            new StructField("ts", DataTypes.TimestampType, true, null),
            new StructField("sum_power", DataTypes.DoubleType, true, null),
            new StructField("sum_voltage", DataTypes.DoubleType, true, null),
            new StructField("sum_current", DataTypes.DoubleType, true, null),
            new StructField("energy_consumed", DataTypes.DoubleType, true, null),
            new StructField("count_agg_rows", DataTypes.LongType, true, null),

    });
    public final static StructType temperatureTableSchema = new StructType(new StructField[]{
            new StructField("granularity", DataTypes.IntegerType, true, null),
            new StructField("sensor_id", DataTypes.StringType, true, null),
            new StructField("ts", DataTypes.TimestampType, true, null),
            new StructField("sum_temperature", DataTypes.DoubleType, true, null),
            new StructField("count_agg_rows", DataTypes.LongType, true, null),
    });

    private final String host;
    private final String user;
    private final String password;
    private final String database;
    private final String tableName;
    private final SchemaType schemaType;
    private final String sensorIdColumnName;
    private final String tsColumnName;
    private final Properties properties;

    public MySQLTable(String host, String user, String password, String database, String tableName, SchemaType schemaType, String sensorIdColumnName, String tsColumnName) {
        this.host = host;
        this.user = user;
        this.password = password;
        this.database = database;
        this.tableName = tableName;
        this.schemaType = schemaType;
        this.sensorIdColumnName = sensorIdColumnName;
        this.tsColumnName = tsColumnName;
        properties = new Properties();
        this.setProperties();
    }

    public String getURL() {
        return "jdbc:mysql://" + host + ":3306/" + database + "?useSSL=false&autoReconnect=true&failOverReadOnly=false&maxReconnects=10&useUnicode=true&characterEncoding=UTF-8";
    }

    public String getURL(String database) {
        return "jdbc:mysql://" + host + ":3306/" + database + "?useSSL=false&autoReconnect=true&failOverReadOnly=false&maxReconnects=10&useUnicode=true&characterEncoding=UTF-8";
    }

    public void setProperties() {
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");
    }

    public Properties getProperties() {
        return this.properties;
    }

    public String getTableName() {
        return tableName;
    }

    public SchemaType getSchemaType() {
        return schemaType;
    }

    public String getSensorIdColumnName() {
        return sensorIdColumnName;
    }

    public String getTsColumnName() {
        return tsColumnName;
    }

    public String getHost() {
        return host;
    }

    public String getDatabase() {
        return database;
    }

    public enum SchemaType {power, temperature}

    @Override
    public String toString() {
        return "MySQLTable{" +
                "host='" + host + '\'' +
                ", database='" + database + '\'' +
                ", tableName='" + tableName + '\'' +
                ", schemaType=" + schemaType +
                ", sensorIdColumnName='" + sensorIdColumnName + '\'' +
                ", tsColumnName='" + tsColumnName + '\'' +
                ", properties=" + properties +
                '}';
    }
}
