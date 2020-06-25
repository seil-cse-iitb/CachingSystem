package controllers;

import beans.*;
import managers.LogManager;
import managers.Utils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.google.gson.*;

import java.util.ArrayList;
import java.util.List;

public class SensorController {
    CacheSystemController c;

    public SensorController(CacheSystemController c) {
        this.c = c;
    }


    public List<Pair<TimeRangeBean, SLCacheTableBean>> getIntersectionTimeRangeVsSLCacheTables(SensorBean sensorBean, TimeRangeBean timeRangeBean) {
        List<Pair<TimeRangeBean, SLCacheTableBean>> pairList = new ArrayList<>();
        for (Pair<TimeRangeBean, SLCacheTableBean> pair : sensorBean.getTimeRangeVsSLCacheTables()) {
            TimeRangeBean intersection = c.timeRangeController.intersection(pair.getLeft(), timeRangeBean);
            if (intersection != null)
                pairList.add(new MutablePair<>(intersection, pair.getRight()));
        }
        return pairList;
    }

    public List<Pair<TimeRangeBean, SourceTableBean>> getIntersectionTimeRangeVsSourceTables(SensorBean sensorBean, TimeRangeBean timeRangeBean) {
        List<Pair<TimeRangeBean, SourceTableBean>> pairList = new ArrayList<>();
        for (Pair<TimeRangeBean, SourceTableBean> pair : sensorBean.getTimeRangeVsSourceTables()) {
            TimeRangeBean intersection = c.timeRangeController.intersection(pair.getLeft(), timeRangeBean);
            if (intersection != null)
                pairList.add(new MutablePair<>(intersection, pair.getRight()));
        }
        return pairList;
    }

    public SensorBean fetchSensor(String sensorId) {
        SensorBean sensorBean = c.cb.sensorBeanMap.get(sensorId);
        if (sensorBean != null) return sensorBean;
        LogManager.logInfo("Fetching Sensor: " + sensorId);
        String url = "http://10.129.6.56:3200/device/serial/" + sensorId;
        String res = Utils.makeGetRequest(url);
        JsonObject sensorJsonObject = ConfigurationController.gson.fromJson(res, JsonObject.class);
        if (sensorJsonObject.get("classId").getAsString().contains("AGGREGATOR")) {
            //sensor group
            SensorGroupBean sensorGroupBean = new SensorGroupBean((SensorGroupBean) c.cb.sensorBeanMap.get("default_sensor_group"));
            sensorGroupBean.getSensorList().clear();
            JsonArray aggregatesArray = sensorJsonObject.getAsJsonObject("meta").getAsJsonArray("aggregates");
            for (int i = 0; i < aggregatesArray.size(); i++) {
                String aggregateId = aggregatesArray.get(i).getAsString();
                SensorBean sensorBean1 = fetchSensor(aggregateId);
                if (sensorBean1 != null)
                    sensorGroupBean.getSensorList().add(sensorBean1);
            }
            sensorBean = sensorGroupBean;
            LogManager.logDebugInfo("SensorGroupBean:"+sensorId);
        } else {
            //normal sensor
            sensorBean = new SensorBean(c.cb.sensorBeanMap.get("default_sensor"));
            LogManager.logDebugInfo("SensorBean:"+sensorId);
        }
        sensorBean.setSensorId(sensorId);
        c.cb.sensorBeanMap.put(sensorId, sensorBean);
        for (int i = 0; i < sensorBean.getTimeRangeVsSLCacheTables().size(); i++) {
            Pair<TimeRangeBean, SLCacheTableBean> timeRangeBeanSLCacheTableBeanPair = sensorBean.getTimeRangeVsSLCacheTables().get(i);
            LogManager.logDebugInfo(timeRangeBeanSLCacheTableBeanPair.getLeft()+":"+timeRangeBeanSLCacheTableBeanPair.getRight());
        }
        for (int i = 0; i < sensorBean.getTimeRangeVsSourceTables().size(); i++) {
            Pair<TimeRangeBean, SourceTableBean> timeRangeBeanSLCacheTableBeanPair = sensorBean.getTimeRangeVsSourceTables().get(i);
            LogManager.logDebugInfo(timeRangeBeanSLCacheTableBeanPair.getLeft()+":"+timeRangeBeanSLCacheTableBeanPair.getRight());
        }
        return sensorBean;
    }
}
