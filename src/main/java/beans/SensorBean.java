package beans;


import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SensorBean {
    public String sensorId;
    public FLCacheTableBean flCacheTableBean;
    public final ArrayList<Pair<TimeRangeBean, SourceTableBean>> timeRangeVsSourceTables = new ArrayList<>();
    public final ArrayList<Pair<TimeRangeBean, SLCacheTableBean>> timeRangeVsSLCacheTables = new ArrayList<>();
    public BitmapBean flBitmapBean,slBitmapBean;

    public  SensorBean(){}

    public SensorBean(SensorBean sensorBean){
        this.sensorId = sensorBean.getSensorId();
        this.flCacheTableBean = sensorBean.getFlCacheTableBean();
        timeRangeVsSourceTables.addAll((Collection<? extends Pair<TimeRangeBean, SourceTableBean>>) sensorBean.getTimeRangeVsSourceTables().clone());
        timeRangeVsSLCacheTables.addAll((Collection<? extends Pair<TimeRangeBean, SLCacheTableBean>>) sensorBean.getTimeRangeVsSLCacheTables().clone());
        flBitmapBean = new BitmapBean(sensorBean.getFlBitmapBean());
        slBitmapBean = new BitmapBean(sensorBean.getSlBitmapBean());
    }
    public ArrayList<Pair<TimeRangeBean, SourceTableBean>> getTimeRangeVsSourceTables() {
        return timeRangeVsSourceTables;
    }

    public ArrayList<Pair<TimeRangeBean, SLCacheTableBean>> getTimeRangeVsSLCacheTables() {
        return timeRangeVsSLCacheTables;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public FLCacheTableBean getFlCacheTableBean() {
        return flCacheTableBean;
    }

    public void setFlCacheTableBean(FLCacheTableBean flCacheTableBean) {
        this.flCacheTableBean = flCacheTableBean;
    }

    public BitmapBean getFlBitmapBean() {
        return flBitmapBean;
    }

    public void setFlBitmapBean(BitmapBean flBitmapBean) {
        this.flBitmapBean = flBitmapBean;
    }

    public BitmapBean getSlBitmapBean() {
        return slBitmapBean;
    }

    public void setSlBitmapBean(BitmapBean slBitmapBean) {
        this.slBitmapBean = slBitmapBean;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SensorBean)) return false;

        SensorBean that = (SensorBean) o;

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
