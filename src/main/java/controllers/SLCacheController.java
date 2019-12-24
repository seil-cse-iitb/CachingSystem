package controllers;

import beans.*;
import managers.LogManager;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class SLCacheController {
    private CacheSystemController c;

    public SLCacheController(CacheSystemController c) {
        this.c=c;
    }

    public void cleanCache(SensorBean sensorBean, GranularityBean specifiedGranularity, TimeRangeBean timeRange) {
        List<Pair<TimeRangeBean, SLCacheTableBean>> intersectionTimeRangeVsSLCacheTables = c.sensorController.getIntersectionTimeRangeVsSLCacheTables(sensorBean, timeRange);
        LogManager.logPriorityInfo("intersectionTimeRange:" + intersectionTimeRangeVsSLCacheTables);
        for (Pair<TimeRangeBean, SLCacheTableBean> slCacheTableBeanPair : intersectionTimeRangeVsSLCacheTables) {
            SLCacheTableBean slCacheTableBean = slCacheTableBeanPair.getRight();
            Connection connection=null;
            try {
                connection = DriverManager.getConnection(c.databaseController.getURL(slCacheTableBean.getDatabaseBean()), c.databaseController.getProperties(slCacheTableBean.getDatabaseBean()));
                PreparedStatement ps = connection.prepareStatement("DELETE from `"+slCacheTableBean.getTableName()+"` where `"+slCacheTableBean.getSensorIdColumnName()+"`=? and `granularityId`=? and `"+slCacheTableBean.getTsColumnName()+"`>=? and `"+slCacheTableBean.getTsColumnName()+"`<?");
                ps.setString(1,sensorBean.getSensorId());
                ps.setString(2,specifiedGranularity.getGranularityId());
                ps.setLong(3,slCacheTableBeanPair.getLeft().startTime);
                ps.setLong(4,slCacheTableBeanPair.getLeft().endTime);
                ps.executeLargeUpdate();
                ps.close();
                connection.close();
            } catch (SQLException e) {
                LogManager.logError("[" + this.getClass() + "][cleanCache][" + slCacheTableBean.getDatabaseBean().getDatabaseId() + "]" + e.getMessage());
            }finally{
                if(connection!=null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        LogManager.logError("[" + this.getClass() + "][connection closing exception]" + e.getMessage());
                    }
                }
            }
        }
    }
}
