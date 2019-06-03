package controllers;

import beans.SLCacheTableBean;
import beans.SensorBean;
import beans.SourceTableBean;
import beans.TimeRangeBean;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

public class SensorController {
    CacheSystemController c;
    public SensorController(CacheSystemController c) {
        this.c = c;
    }


    public List<Pair<TimeRangeBean, SLCacheTableBean>> getIntersectionTimeRangeVsSLCacheTables(SensorBean sensorBean, TimeRangeBean timeRangeBean) {
        List<Pair<TimeRangeBean, SLCacheTableBean>> pairList = new ArrayList<>();
        for(Pair<TimeRangeBean,SLCacheTableBean> pair:sensorBean.getTimeRangeVsSLCacheTables()){
            TimeRangeBean intersection = c.timeRangeController.intersection(pair.getLeft(), timeRangeBean);
            if(intersection!=null)
            pairList.add(new MutablePair<>(intersection,pair.getRight()));
        }
        return pairList;
    }

    public List<Pair<TimeRangeBean, SourceTableBean>> getIntersectionTimeRangeVsSourceTables(SensorBean sensorBean, TimeRangeBean timeRangeBean) {
        List<Pair<TimeRangeBean, SourceTableBean>> pairList = new ArrayList<>();
        for(Pair<TimeRangeBean,SourceTableBean> pair:sensorBean.getTimeRangeVsSourceTables()){
            TimeRangeBean intersection = c.timeRangeController.intersection(pair.getLeft(), timeRangeBean);
            if(intersection!=null)
                pairList.add(new MutablePair<>(intersection,pair.getRight()));
        }
        return pairList;
    }
}
