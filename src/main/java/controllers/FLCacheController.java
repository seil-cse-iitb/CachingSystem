package controllers;

import beans.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static org.apache.spark.sql.functions.col;


public class FLCacheController {
    CacheSystemController c;

    public FLCacheController(CacheSystemController c) {
        this.c = c;
    }

    public void updateCache(SensorBean sensorBean, GranularityBean requiredGranularity, ArrayList<TimeRangeBean> nonExistingDataRanges) {
        c.logManager.logInfo("[Updating cache databases][NonExistingDataRanges:" + nonExistingDataRanges + "]");
        int interval = requiredGranularity.getFetchIntervalAtOnceInSeconds(); // how much data to fetch in each iteration
        int numPartitions = requiredGranularity.getNumPartitionsForEachInterval();// This depends on amount of memory a executor has
        FLCacheTableBean flCacheTableBean = sensorBean.getFlCacheTableBean();
        for (TimeRangeBean timeRangeBean : nonExistingDataRanges) {
            List<Pair<TimeRangeBean, SLCacheTableBean>> intersectionTimeRangeVsSLCacheTables = c.sensorController.getIntersectionTimeRangeVsSLCacheTables(sensorBean, timeRangeBean);
            for (Pair<TimeRangeBean, SLCacheTableBean> slCacheTableBeanPair : intersectionTimeRangeVsSLCacheTables) {
                //fetch this timeRange from this slcachetable
                LinkedList<TimeRangeBean> slNonExistingDataRange = new LinkedList<>();
                slNonExistingDataRange.add(slCacheTableBeanPair.getLeft());
                GranularityBean currentGranularity = requiredGranularity;
                do {
                    TimeRangeBean currentTimeRangeBean = slNonExistingDataRange.pop();
                    if (currentGranularity != null) {
                        //aggregate and update flCacheTable & slCacheTable
                        if(c.bitmapController.isSomeDataAvailable(sensorBean.getSlBitmapBean(),currentGranularity,currentTimeRangeBean)) {
                            c.logManager.logInfo("Data found in SL"+currentTimeRangeBean);
                            updateSLAndFLCacheFromSL(sensorBean, requiredGranularity, slCacheTableBeanPair.getRight(), flCacheTableBean, currentGranularity, currentTimeRangeBean);
                        }
                        slNonExistingDataRange.addAll(c.bitmapController.getNonExistingDataRange(sensorBean.getSlBitmapBean(), currentGranularity, currentTimeRangeBean));
                        currentGranularity = c.granularityController.nextSmallerGranularity(currentGranularity);
                    } else {
                        //currentGranularity will be null when there is no smaller granularity remaining to check in SLCache then aggregate from source
                        //get data from sourceTable update sl and fl both
                        updateSLAndFLCacheFromSource(sensorBean, requiredGranularity, slCacheTableBeanPair.getRight(), flCacheTableBean, currentTimeRangeBean);
                    }
                } while (!slNonExistingDataRange.isEmpty());
            }
        }
    }

    private void updateSLAndFLCacheFromSource(SensorBean sensorBean, GranularityBean requiredGranularity, SLCacheTableBean sl, FLCacheTableBean fl, TimeRangeBean timeRangeBean) {
        List<Pair<TimeRangeBean, SourceTableBean>> intersectionTimeRangeVsSourceTables = c.sensorController.getIntersectionTimeRangeVsSourceTables(sensorBean, timeRangeBean);
        for (Pair<TimeRangeBean, SourceTableBean> sourceTableBeanPair : intersectionTimeRangeVsSourceTables) {
            TimeRangeBean currentTimeRangeBean = sourceTableBeanPair.getLeft();
            SourceTableBean sourceTable = sourceTableBeanPair.getRight();
            c.logManager.logInfo("--[From SourceTable][Aggregation Started]: " + currentTimeRangeBean);
            currentTimeRangeBean.startTime = currentTimeRangeBean.startTime - (currentTimeRangeBean.startTime % requiredGranularity.getGranularityInTermsOfSeconds());
            currentTimeRangeBean.endTime = currentTimeRangeBean.endTime + (requiredGranularity.getGranularityInTermsOfSeconds() - (currentTimeRangeBean.endTime % requiredGranularity.getGranularityInTermsOfSeconds()));
            long i = currentTimeRangeBean.startTime;
            while (i < currentTimeRangeBean.endTime) {
                long startTime = i;
                long endTime = Math.min(startTime + requiredGranularity.getFetchIntervalAtOnceInSeconds(), currentTimeRangeBean.endTime);
                c.logManager.logInfo("----[From SourceTable][" + new Date(startTime * 1000) + "] to [" + new Date(endTime * 1000) + "]");
                Dataset<Row> sourceDataset = c.sparkSession.read()
                        .jdbc(c.databaseController.getURL(sourceTable.getDatabaseBean()), sourceTable.getTableName(), sourceTable.getTsColumnName(), startTime, endTime, requiredGranularity.getNumPartitionsForEachInterval(), c.databaseController.getProperties(sourceTable.getDatabaseBean()));
                Column tsFilter = col(sourceTable.getTsColumnName()).$greater$eq(startTime).and(col(sourceTable.getTsColumnName()).$less(endTime));
                sourceDataset = sourceDataset.filter(col(sourceTable.getSensorIdColumnName()).equalTo(sensorBean.getSensorId())
                        .and(tsFilter));
                //update fl and sl after aggregation
                Dataset<Row> aggregated = c.aggregationManager.aggregateFromSource(sourceDataset, requiredGranularity, sourceTable);
                aggregated = aggregated.cache();
                aggregated
                        .withColumn(sl.getTsColumnName(), col(sourceTable.getTsColumnName()))
                        .withColumn(sl.getSensorIdColumnName(), col(sourceTable.getSensorIdColumnName()))
                        .write().mode(SaveMode.Append).jdbc(c.databaseController.getURL(sl.getDatabaseBean()), sl.getTableName(), c.databaseController.getProperties(sl.getDatabaseBean()));
                aggregated
                        .withColumn(fl.getTsColumnName(), col(sourceTable.getTsColumnName()))
                        .withColumn(fl.getSensorIdColumnName(), col(sourceTable.getSensorIdColumnName()))
                        .write().mode(SaveMode.Append).jdbc(c.databaseController.getURL(fl.getDatabaseBean()), fl.getTableName(), c.databaseController.getProperties(fl.getDatabaseBean()));
                i = endTime;
            }
            c.logManager.logInfo("--[From SourceTable][Aggregation Finished]: " + currentTimeRangeBean);
        }
    }

    private void updateSLAndFLCacheFromSL(SensorBean sensorBean, GranularityBean requiredGranularity, SLCacheTableBean sl, FLCacheTableBean fl, GranularityBean currentGranularity, TimeRangeBean currentTimeRangeBean) {
        c.logManager.logInfo("--[From SLCache][Aggregation Started]: " + currentTimeRangeBean);
        currentTimeRangeBean.startTime = currentTimeRangeBean.startTime - (currentTimeRangeBean.startTime % requiredGranularity.getGranularityInTermsOfSeconds());
        currentTimeRangeBean.endTime = currentTimeRangeBean.endTime + (requiredGranularity.getGranularityInTermsOfSeconds() - (currentTimeRangeBean.endTime % requiredGranularity.getGranularityInTermsOfSeconds()));
        long i = currentTimeRangeBean.startTime;
        while (i < currentTimeRangeBean.endTime) {
            long startTime = i;
            long endTime = Math.min(startTime + requiredGranularity.getFetchIntervalAtOnceInSeconds(), currentTimeRangeBean.endTime);
            c.logManager.logInfo("----[From SLCache][" + new Date(startTime * 1000) + "] to [" + new Date(endTime * 1000) + "]");
            Dataset<Row> slDataset = c.sparkSession.read()
                    .jdbc(c.databaseController.getURL(sl.getDatabaseBean()), sl.getTableName(), sl.getTsColumnName(), startTime, endTime, requiredGranularity.getNumPartitionsForEachInterval(), c.databaseController.getProperties(sl.getDatabaseBean()));
            Column tsFilter = col(sl.getTsColumnName()).$greater$eq(startTime).and(col(sl.getTsColumnName()).$less(endTime));
            slDataset = slDataset.filter(col("granularityId").equalTo(currentGranularity.getGranularityId())
                    .and(col(sl.getSensorIdColumnName()).equalTo(sensorBean.getSensorId()))
                    .and(tsFilter));
            if (currentGranularity == requiredGranularity) {
                //update fl only without aggregation i.e. just copy paste
                slDataset.write().mode(SaveMode.Append).jdbc(c.databaseController.getURL(fl.getDatabaseBean()), fl.getTableName(), c.databaseController.getProperties(fl.getDatabaseBean()));
            } else {
                //update fl and sl after aggregation
                Dataset<Row> aggregated = c.aggregationManager.aggregateFromSL(slDataset, requiredGranularity, sl);
                aggregated = aggregated.cache();
                aggregated.write().mode(SaveMode.Append).jdbc(c.databaseController.getURL(sl.getDatabaseBean()), sl.getTableName(), c.databaseController.getProperties(sl.getDatabaseBean()));
                aggregated
                        .withColumn(fl.getTsColumnName(), col(sl.getTsColumnName()))
                        .withColumn(fl.getSensorIdColumnName(), col(sl.getSensorIdColumnName()))
                        .write().mode(SaveMode.Append).jdbc(c.databaseController.getURL(fl.getDatabaseBean()), fl.getTableName(), c.databaseController.getProperties(fl.getDatabaseBean()));
            }
            i = endTime;
        }
        c.logManager.logInfo("--[From SLCache][Aggregation Finished]: " + currentTimeRangeBean);
    }

}
