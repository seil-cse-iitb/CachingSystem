package controllers;

import beans.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.BitSet;

import static managers.Utils.getTimeInSec;

public class BitmapController {
    CacheSystemController c;

    public BitmapController(CacheSystemController c) {
        this.c = c;
    }

    public void initBitmapsIfNotExists(SensorBean sensorBean){
        FLCacheTableBean flc = sensorBean.getFlCacheTableBean();
        try {
            Connection connection = DriverManager.getConnection(c.databaseController.getURL(flc.getDatabaseBean()), c.databaseController.getProperties(flc.getDatabaseBean()));
            String tableName = flc.getTableName() + "_" + c.cb.bitmapTableNameSuffix;
            int i = connection.prepareStatement(String.format("create table if not exists %s (%s varchar(200), granularity varchar(50), bitmapStartTime long, bitmapEndTime long, fl_bitset mediumblob, sl_bitset mediumblob, CONSTRAINT PK_%s PRIMARY KEY (%s,granularity))", tableName, flc.getSensorIdColumnName(), tableName, flc.getSensorIdColumnName()))
                    .executeUpdate();
            assert i == 0;

            for (GranularityBean granularity : c.cb.granularityBeanMap.values()) {
                Blob flBlob = connection.createBlob();
                flBlob.setBytes(1, sensorBean.getFlBitmapBean().granularityBeanBitSetMap.get(granularity).toByteArray());
                Blob slBlob = connection.createBlob();
                slBlob.setBytes(1, sensorBean.getSlBitmapBean().granularityBeanBitSetMap.get(granularity).toByteArray());

                PreparedStatement preparedStatement = connection.prepareStatement(String.format("insert ignore into %s (%s,granularity,bitmapStartTime,bitmapEndTime,fl_bitset,sl_bitset) values(?,?,?,?,?,?)", tableName, flc.getSensorIdColumnName()));
                preparedStatement.setString(1, sensorBean.getSensorId());
                preparedStatement.setString(2, granularity.getGranularityId());
                preparedStatement.setLong(3, getTimeInSec(sensorBean.getFlBitmapBean().startTime));
                preparedStatement.setLong(4, getTimeInSec(sensorBean.getFlBitmapBean().endTime));
                preparedStatement.setBlob(5, flBlob);
                preparedStatement.setBlob(6, slBlob);
                preparedStatement.close();
            }
            connection.close();
        } catch (SQLException e) {
            c.logManager.logError("[" + this.getClass() + "][initBitmapsIfNotExists]" + e.getMessage());
        }
    }
    public void initBitmapsIfNotExists() {
        c.logManager.logInfo("[Initializing bitmap for all sensors]");
        for (SensorBean sensorBean : c.cb.sensorBeanMap.values()) {
            initBitmapsIfNotExists(sensorBean);
        }
    }

    public void loadBitmaps(SensorBean sensorBean) {
        c.logManager.logInfo("[Loading bitmap for sensor: " + sensorBean.getSensorId() + "]");
        FLCacheTableBean flc = sensorBean.getFlCacheTableBean();
        try {
            Connection connection = DriverManager.getConnection(c.databaseController.getURL(flc.getDatabaseBean()), c.databaseController.getProperties(flc.getDatabaseBean()));
            String tableName = flc.getTableName() + "_" + c.cb.bitmapTableNameSuffix;
            PreparedStatement preparedStatement = connection.prepareStatement(String.format("select %s,granularity,fl_bitset,sl_bitset from %s where %s='%s'", flc.getSensorIdColumnName(), tableName, flc.getSensorIdColumnName(), sensorBean.getSensorId()));
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String granularity = resultSet.getString("granularity");
                Blob flBlob = resultSet.getBlob("fl_bitset");
                BitSet flBitset = BitSet.valueOf(flBlob.getBytes(1, (int) flBlob.length()));
                Blob slBlob = resultSet.getBlob("sl_bitset");
                BitSet slBitset = BitSet.valueOf(slBlob.getBytes(1, (int) flBlob.length()));
                sensorBean.getFlBitmapBean().granularityBeanBitSetMap.put(c.cb.granularityBeanMap.get(granularity), flBitset);
                sensorBean.getSlBitmapBean().granularityBeanBitSetMap.put(c.cb.granularityBeanMap.get(granularity), slBitset);
            }
            resultSet.close();
            preparedStatement.close();
            connection.close();
        } catch (SQLException e) {
            c.logManager.logError("[" + this.getClass() + "][LoadBitmap]" + e.getMessage());
        }
    }

    public void loadBitmaps() {
        c.logManager.logInfo("[Loading bitmap for all sensors]");
        for (SensorBean sensorBean : c.cb.sensorBeanMap.values()) {
            loadBitmaps(sensorBean);
        }
    }

    public void saveBitmaps(SensorBean sensorBean) {
        if (!(sensorBean.getFlBitmapBean().isDirty || sensorBean.getSlBitmapBean().isDirty)) {
            return;
        }
        FLCacheTableBean flc = sensorBean.getFlCacheTableBean();
        try {
            Connection connection = DriverManager.getConnection(c.databaseController.getURL(flc.getDatabaseBean()), c.databaseController.getProperties(flc.getDatabaseBean()));
            String tableName = flc.getTableName() + "_" + c.cb.bitmapTableNameSuffix;
            int i = connection.prepareStatement(String.format("create table if not exists %s (%s varchar(200), granularity varchar(50), bitmapStartTime long, bitmapEndTime long, fl_bitset mediumblob, sl_bitset mediumblob, CONSTRAINT PK_%s PRIMARY KEY (%s,granularity))", tableName, flc.getSensorIdColumnName(), tableName, flc.getSensorIdColumnName()))
                    .executeUpdate();
            assert i == 0;

            for (GranularityBean granularity : c.cb.granularityBeanMap.values()) {
                Blob flBlob = connection.createBlob();
                flBlob.setBytes(1, sensorBean.getFlBitmapBean().granularityBeanBitSetMap.get(granularity).toByteArray());
                Blob slBlob = connection.createBlob();
                slBlob.setBytes(1, sensorBean.getSlBitmapBean().granularityBeanBitSetMap.get(granularity).toByteArray());

                PreparedStatement preparedStatement = connection.prepareStatement(String.format("replace into %s (%s,granularity,bitmapStartTime,bitmapEndTime,fl_bitset,sl_bitset) values(?,?,?,?,?,?)", tableName, flc.getSensorIdColumnName()));
                preparedStatement.setString(1, sensorBean.getSensorId());
                preparedStatement.setString(2, granularity.getGranularityId());
                preparedStatement.setLong(3, getTimeInSec(sensorBean.getFlBitmapBean().startTime));
                preparedStatement.setLong(4, getTimeInSec(sensorBean.getFlBitmapBean().endTime));
                preparedStatement.setBlob(5, flBlob);
                preparedStatement.setBlob(6, slBlob);
                assert preparedStatement.executeUpdate() >= 1;
                preparedStatement.close();
            }
            connection.close();
        } catch (SQLException e) {
            c.logManager.logError("[" + this.getClass() + "][SaveBitmap]" + e.getMessage());
        }
    }

    public void saveBitmaps() {
        for (SensorBean sensorBean : c.cb.sensorBeanMap.values()) {
            saveBitmaps(sensorBean);
        }
    }

    public ArrayList<TimeRangeBean> getNonExistingDataRange(BitmapBean bitmapBean, GranularityBean granularityBean, TimeRangeBean timeRange) {
        BitSet bitSet = bitmapBean.granularityBeanBitSetMap.get(granularityBean);
        ArrayList<TimeRangeBean> nonExistingTimeRanges = new ArrayList<>();
        int startI = (int) (timeRange.startTime - getTimeInSec(bitmapBean.startTime)) / granularityBean.getGranularityInTermsOfSeconds();
        int endI = (int) (timeRange.endTime - getTimeInSec(bitmapBean.startTime)) / granularityBean.getGranularityInTermsOfSeconds();
        int progressI = startI;
        while (progressI < endI) {
            int clearBitI = bitSet.nextClearBit(progressI);
            if (clearBitI == -1 || clearBitI >= endI) break;
            int setBitI = bitSet.nextSetBit(clearBitI);
            if (setBitI == -1 || setBitI > endI) {
                setBitI = endI;
            }
            nonExistingTimeRanges.add(new TimeRangeBean(clearBitI * granularityBean.getGranularityInTermsOfSeconds() + getTimeInSec(bitmapBean.startTime),
                    setBitI * granularityBean.getGranularityInTermsOfSeconds() + getTimeInSec(bitmapBean.startTime)));
            progressI = setBitI;
        }
        return nonExistingTimeRanges;
    }

    public boolean isSomeDataAvailable(BitmapBean bitmapBean, GranularityBean granularityBean, TimeRangeBean timeRange) {
        BitSet bitSet = bitmapBean.granularityBeanBitSetMap.get(granularityBean);
        int startI = (int) (timeRange.startTime - getTimeInSec(bitmapBean.startTime)) / granularityBean.getGranularityInTermsOfSeconds();
        int endI = (int) (timeRange.endTime - getTimeInSec(bitmapBean.startTime)) / granularityBean.getGranularityInTermsOfSeconds();
        int setBit = bitSet.nextSetBit(startI);
        if (endI >= setBit && setBit != -1) return true;
        return false;
    }

    public void updateBitmap(BitmapBean bitmapBean, GranularityBean granularityBean, ArrayList<TimeRangeBean> timeRanges) {
        c.logManager.logInfo("[Updating Bitmap][Granularity:" + granularityBean.getGranularityId() + "]");
        BitSet bitSet = bitmapBean.granularityBeanBitSetMap.get(granularityBean);
        for (TimeRangeBean timeRange : timeRanges) {
            int start = (int) ((timeRange.startTime - getTimeInSec(bitmapBean.startTime)) / granularityBean.getGranularityInTermsOfSeconds());
            int end = (int) (timeRange.endTime - getTimeInSec(bitmapBean.startTime)) / granularityBean.getGranularityInTermsOfSeconds();
            bitSet.set(start, end);
        }
        bitmapBean.isDirty = true;
    }


}
