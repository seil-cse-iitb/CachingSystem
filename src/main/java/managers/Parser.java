package managers;

import beans.ConfigurationBean;
import beans.DatabaseBean;
import beans.SourceTableBean;
import controllers.CacheSystemController;
import controllers.DatabaseController;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;


public class Parser implements Serializable {
    private StructType power = new StructType(new StructField[]{
        new StructField("id", DataTypes.IntegerType,false,null),
        new StructField("topic", DataTypes.StringType,false,null),
        new StructField("timestamp", DataTypes.TimestampType,false,null),
        new StructField("sensor_id", DataTypes.StringType,false,null),
        new StructField("ignore_1", DataTypes.DoubleType,true,null), // index: 2
        new StructField("ts", DataTypes.DoubleType,true,null), // index: 16
        new StructField("ignore_3", DataTypes.DoubleType,true,null), // index: 23
        new StructField("ignore_4", DataTypes.DoubleType,true,null), // index: 30
        new StructField("ignore_5", DataTypes.DoubleType,true,null), // index: 17
        new StructField("ignore_6", DataTypes.DoubleType,true,null), // index: 24
        new StructField("ignore_7", DataTypes.DoubleType,true,null), // index: 31
        new StructField("ignore_8", DataTypes.DoubleType,true,null), // index: 12
        new StructField("ignore_9", DataTypes.DoubleType,true,null), // index: 19
        new StructField("ignore_10", DataTypes.DoubleType,true,null), // index: 26
        new StructField("ignore_11", DataTypes.DoubleType,true,null), // index: 14
        new StructField("power_1", DataTypes.DoubleType,true,null), // index: 21
        new StructField("ignore_13", DataTypes.DoubleType,true,null), // index: 28
        new StructField("power_factor_1", DataTypes.DoubleType,true,null), // index: 33
        new StructField("ignore_15", DataTypes.DoubleType,true,null), // index: 1
        new StructField("voltage_1", DataTypes.DoubleType,true,null), // index: 3
        new StructField("current_1", DataTypes.DoubleType,true,null), // index: 4
        new StructField("ignore_18", DataTypes.DoubleType,true,null), // index: 5
        new StructField("power_2", DataTypes.DoubleType,true,null), // index: 6
        new StructField("ignore_20", DataTypes.DoubleType,true,null), // index: 7
        new StructField("power_factor_2", DataTypes.DoubleType,true,null), // index: 8
        new StructField("ignore_22", DataTypes.DoubleType,true,null), // index: 9
        new StructField("voltage_2", DataTypes.DoubleType,true,null), // index: 10
        new StructField("current_2", DataTypes.DoubleType,true,null), // index: 11
        new StructField("ignore_25", DataTypes.DoubleType,true,null), // index: 13
        new StructField("power_3", DataTypes.DoubleType,true,null), // index: 15
        new StructField("ignore_27", DataTypes.DoubleType,true,null), // index: 18
        new StructField("power_factor_3", DataTypes.DoubleType,true,null), // index: 20
        new StructField("ignore_29", DataTypes.DoubleType,true,null), // index: 22
        new StructField("voltage_3", DataTypes.DoubleType,true,null), // index: 25
        new StructField("current_3", DataTypes.DoubleType,true,null), // index: 27
        new StructField("ignore_32", DataTypes.DoubleType,true,null), // index: 29
        new StructField("energy_consumed", DataTypes.DoubleType,true,null), // index: 32
        new StructField("ignore_34", DataTypes.DoubleType,true,null), // index: 34
        new StructField("ignore_35", DataTypes.DoubleType,true,null), // index: 35
    });
    ConfigurationBean.SchemaType schemaType;
    public Parser(ConfigurationBean.SchemaType schemaType) {
        this.schemaType = schemaType;
    }

    public Row parse(Integer id, final String topic, Timestamp timestamp, final String sensorId,final String data){
        ArrayList<Object>  list= new ArrayList<>();
        list.add(id);
        list.add(topic);
        list.add(timestamp);
        list.add(sensorId);
        String[] split = data.split(",");
        for (String str :
                split) {
            list.add(Double.parseDouble(str));
        }
        assert list.size()!=39;
        return RowFactory.create(list.toArray());
    }

    public StructType getStructType(){
        switch (schemaType){
            case power:
                return this.power;
            default:
                return this.power;
        }
    }

    public SourceTableBean getSourceTableSchema() {
        SourceTableBean sourceTableBean =  new SourceTableBean();
        sourceTableBean.setSchemaType(schemaType);
        sourceTableBean.setSensorIdColumnName("sensor_id");
        sourceTableBean.setTsColumnName("ts");
        return sourceTableBean;
    }

    public Encoder<Row> getEncoder() {
        return RowEncoder.apply(getStructType());
    }

    public ForeachWriter<Row> getForeachWriter() {

        DatabaseBean databaseBean = new DatabaseBean();
        databaseBean.setDatabaseName("temp_db");
        databaseBean.setHost("10.129.149.24");
        databaseBean.setPort("3306");
        databaseBean.setDatabaseId("temp_db");
        databaseBean.setUsername("root");
        databaseBean.setPassword("MySQL@seil");
        databaseBean.setDatabaseType("SQL");
        return new ForeachWriter<Row>() {
            Connection mysqlConnection = null;

            public String getURL(DatabaseBean db) {
                return "jdbc:mysql://" + db.getHost() + ":" + db.getPort() + "/" + db.getDatabaseName() + "?useSSL=false&autoReconnect=true&failOverReadOnly=false&maxReconnects=10&useUnicode=true&characterEncoding=UTF-8";
            }

            public Connection buildConnection(DatabaseBean db) {
                Connection con = null;
                try {
                    con = DriverManager.getConnection(getURL(db), db.getUsername(), db.getPassword());
                } catch (Exception e) {
                }
                return con;
            }


            @Override
            public boolean open(long l, long l1) {
                mysqlConnection = buildConnection(databaseBean);
                return true;
            }

            @Override
            public void process(Row row) {
                String[] fieldNames = row.schema().fieldNames();
                Object[] values = new Object[fieldNames.length];
                for (int i = 0; i < fieldNames.length; i++) {
                    Object value = row.get(i);
                    if (value != null)
                        values[i] = "" + value + "";
                    else
                        values[i] = "NULL";
                }

                String insertSQL = "insert into temp_table(message) values('"+ StringUtils.join(values, ",")+"')";
                try {
                    Statement statement = mysqlConnection.createStatement();
                    int result = statement.executeUpdate(insertSQL);
                    if(result<1)throw new Exception("Update failed");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void close(Throwable throwable) {
                try {
                    mysqlConnection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        };
    }
}


/*
 [
    {"field_number":2,"field_name":"ts","field_type":"F"},
    {"field_number":16,"field_name":"voltage_1","field_type":"F"},
    {"field_number":23,"field_name":"voltage_2","field_type":"F"},
    {"field_number":30,"field_name":"voltage_3","field_type":"F"},
    {"field_number":17,"field_name":"current_1","field_type":"F"},
    {"field_number":24,"field_name":"current_2","field_type":"F"},
    {"field_number":31,"field_name":"current_3","field_type":"F"},
    {"field_number":12,"field_name":"power_1","field_type":"F"},
    {"field_number":19,"field_name":"power_2","field_type":"F"},
    {"field_number":26,"field_name":"power_3","field_type":"F"},
    {"field_number":14,"field_name":"power_factor_1","field_type":"F"},
    {"field_number":21,"field_name":"power_factor_2","field_type":"F"},
    {"field_number":28,"field_name":"power_factor_3","field_type":"F"},
    {"field_number":33,"field_name":"energy_consumed","field_type":"F"},

    {"field_number":1,"field_name":"ignore_1","field_type":"I"},
    {"field_number":3,"field_name":"ignore_3","field_type":"F"},
    {"field_number":4,"field_name":"ignore_4","field_type":"F"},
    {"field_number":5,"field_name":"ignore_5","field_type":"F"},
    {"field_number":6,"field_name":"ignore_6","field_type":"F"},
    {"field_number":7,"field_name":"ignore_7","field_type":"F"},
    {"field_number":8,"field_name":"ignore_8","field_type":"F"},
    {"field_number":9,"field_name":"ignore_9","field_type":"F"},
    {"field_number":10,"field_name":"ignore_10","field_type":"F"},
    {"field_number":11,"field_name":"ignore_11","field_type":"F"},
    {"field_number":13,"field_name":"ignore_13","field_type":"F"},
    {"field_number":15,"field_name":"ignore_15","field_type":"F"},
    {"field_number":18,"field_name":"ignore_18","field_type":"F"},
    {"field_number":20,"field_name":"ignore_20","field_type":"F"},
    {"field_number":22,"field_name":"ignore_22","field_type":"F"},
    {"field_number":25,"field_name":"ignore_25","field_type":"F"},
    {"field_number":27,"field_name":"ignore_27","field_type":"F"},
    {"field_number":29,"field_name":"ignore_29","field_type":"F"},
    {"field_number":32,"field_name":"ignore_32","field_type":"F"},
    {"field_number":34,"field_name":"ignore_34","field_type":"F"},
    {"field_number":35,"field_name":"ignore_35","field_type":"F"}]

* */