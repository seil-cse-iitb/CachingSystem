package beans;

import managers.Utils;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;
import java.util.Date;

public class LiveStreamBean {
    private Long startTime;
    private Long bitmapUpdatedByStreamTill;
    private AccumulatorV2<Long,Long> accumulator;
    private SensorBean sensorBean;
    private Thread liveStreamThread;
    private String topic;

    public LiveStreamBean(SensorBean sensorBean){
        this.sensorBean = sensorBean;
        this.topic=fetchTopic();
    }

    private String fetchTopic() {
        return "SCHNEIDERMETER/power_k_seil_a";
    }

    public void initStreamParameters(){
        this.startTime = Utils.getTimeInSec(new Date());
        this.bitmapUpdatedByStreamTill = this.startTime;
        this.accumulator = new LongAccumulator();
        this.accumulator.add(this.bitmapUpdatedByStreamTill);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LiveStreamBean that = (LiveStreamBean) o;

        return sensorBean.equals(that.sensorBean);
    }

    @Override
    public int hashCode() {
        return sensorBean.hashCode();
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getBitmapUpdatedByStreamTill() {
        return bitmapUpdatedByStreamTill;
    }

    public void setBitmapUpdatedByStreamTill(Long bitmapUpdatedByStreamTill) {
        this.bitmapUpdatedByStreamTill = bitmapUpdatedByStreamTill;
    }

    public AccumulatorV2<Long, Long> getAccumulator() {
        return accumulator;
    }

    public void setAccumulator(AccumulatorV2<Long, Long> accumulator) {
        this.accumulator = accumulator;
    }

    public SensorBean getSensorBean() {
        return sensorBean;
    }

    public void setSensorBean(SensorBean sensorBean) {
        this.sensorBean = sensorBean;
    }

    public Thread getLiveStreamThread() {
        return liveStreamThread;
    }

    public void setLiveStreamThread(Thread liveStreamThread) {
        this.liveStreamThread = liveStreamThread;
    }

    @Override
    public String toString() {
        return "LiveStreamBean{" +
                "sensorBean=" + sensorBean +
                ", topic=" + topic +
                ", startTime=" + startTime +
                ", bitmapUpdatedByStreamTill=" + bitmapUpdatedByStreamTill +
                ", accumulator=" + accumulator +
                '}';
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
