package manager;

public class Sensor {

    final String sensorId;
    final MySQLTable sourceMySQLTable;
    final MySQLTable cacheMySQLTable;
    final Bitmap bitmap;

    public Sensor(String sensorId, MySQLTable sourceMySQLTable, MySQLTable cacheMySQLTable, Bitmap bitmap) {
        this.sensorId = sensorId;
        this.sourceMySQLTable = sourceMySQLTable;
        this.cacheMySQLTable = cacheMySQLTable;
        this.bitmap = bitmap;
    }

    @Override
    public String toString() {
        return "Sensor{" +
                "sensorId='" + sensorId + '\'' +
                ", sourceMySQLTable=" + sourceMySQLTable +
                ", cacheMySQLTable=" + cacheMySQLTable +
                ", bitmap=" + bitmap +
                '}';
    }
}
