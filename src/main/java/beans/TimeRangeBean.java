package beans;

import java.util.Date;

public class TimeRangeBean {
    public Long startTime; //in seconds must not be null
    public Long endTime;//in seconds must not be null

    public TimeRangeBean(Long startTime, Long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
        assert startTime<=endTime;
    }

    @Override
    public String toString() {
        return "TimeRange{" + new Date(startTime * 1000) + " to " + new Date(endTime * 1000) + '}';
    }


}

