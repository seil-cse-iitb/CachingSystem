package controllers;

import beans.ConfigurationBean;
import beans.TimeRangeBean;

public class TimeRangeController {
    public TimeRangeController(CacheSystemController cb) {
    }

    public TimeRangeBean intersection(TimeRangeBean trb1, TimeRangeBean trb2) {
        if(trb1.endTime<trb2.startTime || trb2.endTime<trb1.startTime)return null;
        else return new TimeRangeBean(trb1.startTime>trb2.startTime?trb1.startTime:trb2.startTime,trb1.endTime>trb2.endTime?trb2.endTime:trb1.endTime);
    }

}
