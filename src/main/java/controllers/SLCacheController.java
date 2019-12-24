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

    public void cleanCache(SensorBean sensorBean, TimeRangeBean timeRangeBean) {
        List<Pair<TimeRangeBean, SLCacheTableBean>> intersectionTimeRangeVsSLCacheTables = c.sensorController.getIntersectionTimeRangeVsSLCacheTables(sensorBean, timeRangeBean);
        LogManager.logPriorityInfo("intersectionTimeRange:" + intersectionTimeRangeVsSLCacheTables);
        for (Pair<TimeRangeBean, SLCacheTableBean> slCacheTableBeanPair : intersectionTimeRangeVsSLCacheTables) {
            SLCacheTableBean slCacheTableBean = slCacheTableBeanPair.getRight();
            try {
                Connection connection = DriverManager.getConnection(c.databaseController.getURL(slCacheTableBean.getDatabaseBean()), c.databaseController.getProperties(slCacheTableBean.getDatabaseBean()));
                PreparedStatement ps = connection.prepareStatement("DELETE from `"+slCacheTableBean.getTableName()+"` where `"+slCacheTableBean.getSensorIdColumnName()+"`=? and `"+slCacheTableBean.getTsColumnName()+"`>=? and `"+slCacheTableBean.getTsColumnName()+"`<?");
                ps.setString(1,sensorBean.getSensorId());
                ps.setLong(2,slCacheTableBeanPair.getLeft().startTime);
                ps.setLong(3,slCacheTableBeanPair.getLeft().endTime);
                ps.executeLargeUpdate();
                ps.close();
                connection.close();
            } catch (SQLException e) {
                LogManager.logError("[" + this.getClass() + "][cleanCache][" + slCacheTableBean.getDatabaseBean().getDatabaseId() + "]" + e.getMessage());
            }
        }
    }
}
