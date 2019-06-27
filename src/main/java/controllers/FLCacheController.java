package controllers;

import beans.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.apache.spark.sql.functions.col;


public class FLCacheController {
    CacheSystemController c;

    public FLCacheController(CacheSystemController c) {
        this.c = c;
    }

    public void updateCache(SensorBean sensorBean, GranularityBean requiredGranularity, ArrayList<TimeRangeBean> nonExistingDataRanges) {
        c.logManager.logInfo("[Updating cache databases][NonExistingDataRanges:" + nonExistingDataRanges + "]");
        FLCacheTableBean flCacheTableBean = sensorBean.getFlCacheTableBean();
        for (TimeRangeBean timeRangeBean : nonExistingDataRanges) {
            List<Pair<TimeRangeBean, SLCacheTableBean>> intersectionTimeRangeVsSLCacheTables = c.sensorController.getIntersectionTimeRangeVsSLCacheTables(sensorBean, timeRangeBean);
            c.logManager.logPriorityInfo("intersectionTimeRange:"+intersectionTimeRangeVsSLCacheTables);
            for (Pair<TimeRangeBean, SLCacheTableBean> slCacheTableBeanPair : intersectionTimeRangeVsSLCacheTables) {
                //fetch this timeRange from this slcachetable
                ArrayList<TimeRangeBean> slNonExistingTimeRangeBeans = new ArrayList<>();
                slNonExistingTimeRangeBeans.add(slCacheTableBeanPair.getLeft());
                for (GranularityBean currentGranularity = requiredGranularity; currentGranularity != null; currentGranularity = c.granularityController.nextSmallerGranularity(currentGranularity)) {
                    c.logManager.logPriorityInfo("Checking granularity: "+currentGranularity.getGranularityId());
                    ArrayList<TimeRangeBean> newTimeRangeBeans = new ArrayList<>();
                    for (TimeRangeBean currentTimeRangeBean : slNonExistingTimeRangeBeans) {
                        //aggregate and update flCacheTable & slCacheTable
                        ArrayList<TimeRangeBean> existingDataRanges = c.bitmapController.getExistingDataRange(sensorBean.getSlBitmapBean(), currentGranularity, currentTimeRangeBean);
                        c.logManager.logPriorityInfo("ExistingDataRange:"+existingDataRanges);
                        ArrayList<TimeRangeBean> nonExistingDataRange = c.bitmapController.getNonExistingDataRange(sensorBean.getSlBitmapBean(), currentGranularity, currentTimeRangeBean);
                        c.logManager.logPriorityInfo("NonExistingDataRange:"+nonExistingDataRange);
                        for (TimeRangeBean existingTimeRangeBean : existingDataRanges) {
                            c.logManager.logPriorityInfo("Data found in SL " + existingTimeRangeBean);
                            updateSLAndFLCacheFromSL(sensorBean, requiredGranularity, slCacheTableBeanPair.getRight(), flCacheTableBean, currentGranularity, existingTimeRangeBean);
                        }
                        newTimeRangeBeans.addAll(c.bitmapController.getNonExistingDataRange(sensorBean.getSlBitmapBean(), currentGranularity, currentTimeRangeBean));
                    }
                    slNonExistingTimeRangeBeans = newTimeRangeBeans;
                }

                for (TimeRangeBean currentTimeRangeBean : slNonExistingTimeRangeBeans) {
                    updateSLAndFLCacheFromSource(sensorBean, requiredGranularity, slCacheTableBeanPair.getRight(), flCacheTableBean, currentTimeRangeBean);
                }
            }
        }
    }

    private void updateSLAndFLCacheFromSource(SensorBean sensorBean, GranularityBean requiredGranularity, SLCacheTableBean sl, FLCacheTableBean fl, TimeRangeBean timeRangeBean) {
        c.logManager.logPriorityInfo("[updateSLAndFLCacheFromSource]");
        GranularityBean smallerGranularity = c.granularityController.nextSmallerGranularity(requiredGranularity);
        List<Pair<TimeRangeBean, SourceTableBean>> intersectionTimeRangeVsSourceTables = c.sensorController.getIntersectionTimeRangeVsSourceTables(sensorBean, timeRangeBean);
        for (Pair<TimeRangeBean, SourceTableBean> sourceTableBeanPair : intersectionTimeRangeVsSourceTables) {
            TimeRangeBean currentTimeRangeBean = sourceTableBeanPair.getLeft();
            SourceTableBean sourceTable = sourceTableBeanPair.getRight();
            c.logManager.logPriorityInfo("--[From SourceTable][Aggregation Started]: " + currentTimeRangeBean);
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
                Dataset<Row> aggregatedRequiredGranularity;
                //if smaller granularity available fetch smaller granularity and required granularity from source if not then fetch required granularity only
                if (smallerGranularity != null) {
                    Dataset<Row> aggregatedSmallerGranularity = c.aggregationManager.aggregateFromSource(sourceDataset, smallerGranularity, sourceTable);
                    aggregatedSmallerGranularity = aggregatedSmallerGranularity
                            .withColumn(sl.getTsColumnName(), col(sourceTable.getTsColumnName()))
                            .withColumn(sl.getSensorIdColumnName(), col(sourceTable.getSensorIdColumnName()));
                    aggregatedSmallerGranularity = aggregatedSmallerGranularity.cache();
                    aggregatedSmallerGranularity.write().mode(SaveMode.Append).jdbc(c.databaseController.getURL(sl.getDatabaseBean()), sl.getTableName(), c.databaseController.getProperties(sl.getDatabaseBean()));
                    aggregatedRequiredGranularity = c.aggregationManager.aggregateFromSL(aggregatedSmallerGranularity, requiredGranularity, sl);
                } else {
                    aggregatedRequiredGranularity = c.aggregationManager.aggregateFromSource(sourceDataset, requiredGranularity, sourceTable);
                }
                aggregatedRequiredGranularity = aggregatedRequiredGranularity.cache();
                aggregatedRequiredGranularity
                        .withColumn(sl.getTsColumnName(), col(sourceTable.getTsColumnName()))
                        .withColumn(sl.getSensorIdColumnName(), col(sourceTable.getSensorIdColumnName()))
                        .write().mode(SaveMode.Append).jdbc(c.databaseController.getURL(sl.getDatabaseBean()), sl.getTableName(), c.databaseController.getProperties(sl.getDatabaseBean()));
                aggregatedRequiredGranularity
                        .withColumn(fl.getTsColumnName(), col(sourceTable.getTsColumnName()))
                        .withColumn(fl.getSensorIdColumnName(), col(sourceTable.getSensorIdColumnName()))
                        .write().mode(SaveMode.Append).jdbc(c.databaseController.getURL(fl.getDatabaseBean()), fl.getTableName(), c.databaseController.getProperties(fl.getDatabaseBean()));
                i = endTime;
            }
            c.logManager.logInfo("--[From SourceTable][Aggregation Finished]: " + currentTimeRangeBean);
        }
        if (smallerGranularity != null) {
            //update bitmap of SL if smaller granularity available
            c.logManager.logPriorityInfo("[updateSLAndFLCacheFromSource]Bitmap update for " + smallerGranularity.getGranularityId() + " in " + timeRangeBean);
            c.bitmapController.updateBitmap(sensorBean.getSlBitmapBean(), smallerGranularity, timeRangeBean);
            assert c.bitmapController.getNonExistingDataRange(sensorBean.getSlBitmapBean(),smallerGranularity,timeRangeBean).isEmpty();
        }
    }

    private void updateSLAndFLCacheFromSL(SensorBean sensorBean, GranularityBean requiredGranularity, SLCacheTableBean sl, FLCacheTableBean fl, GranularityBean currentGranularity, TimeRangeBean currentTimeRangeBean) {
        c.logManager.logPriorityInfo("[updateSLAndFLCacheFromSL]");
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
                //for smaller granularity of requiredgranularity
                Dataset<Row> aggregated = c.aggregationManager.aggregateFromSL(slDataset, requiredGranularity, sl);
                //update fl and sl after aggregation
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
