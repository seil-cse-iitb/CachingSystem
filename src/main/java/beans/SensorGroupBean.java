package beans;


import java.util.ArrayList;
import java.util.List;

public class SensorGroupBean extends SensorBean {
    final List<SensorBean> sensorList = new ArrayList<>();
    String spatialAggFunction;

    public String getSpatialAggFunction() {
        return spatialAggFunction;
    }


    public void setSpatialAggFunction(String spatialAggFunction) {
        this.spatialAggFunction = spatialAggFunction;
    }

    public List<SensorBean> getSensorList() {
        return sensorList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SensorGroupBean)) return false;
        SensorGroupBean that = (SensorGroupBean) o;
        return getSensorId().equals(that.getSensorId());
    }

    @Override
    public String toString() {
        return sensorId;
    }

    @Override
    public int hashCode() {
        return getSensorId().hashCode();
    }
}
