package beans;
import static beans.ConfigurationBean.SchemaType;
public class SourceTableBean {
    DatabaseBean databaseBean;
    String tableId,tableName;
    SchemaType schemaType;
    String sensorIdColumnName;
    String tsColumnName;

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public DatabaseBean getDatabaseBean() {
        return databaseBean;
    }

    public void setDatabaseBean(DatabaseBean databaseBean) {
        this.databaseBean = databaseBean;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public SchemaType getSchemaType() {
        return schemaType;
    }

    public void setSchemaType(SchemaType schemaType) {
        this.schemaType = schemaType;
    }

    public String getSensorIdColumnName() {
        return sensorIdColumnName;
    }

    public void setSensorIdColumnName(String sensorIdColumnName) {
        this.sensorIdColumnName = sensorIdColumnName;
    }

    public String getTsColumnName() {
        return tsColumnName;
    }

    public void setTsColumnName(String tsColumnName) {
        this.tsColumnName = tsColumnName;
    }

    @Override
    public String toString() {
        return "SourceTableBean{" +
                "databaseBean=" + databaseBean +
                ", tableId='" + tableId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", schemaType=" + schemaType +
                ", sensorIdColumnName='" + sensorIdColumnName + '\'' +
                ", tsColumnName='" + tsColumnName + '\'' +
                '}';
    }
}
