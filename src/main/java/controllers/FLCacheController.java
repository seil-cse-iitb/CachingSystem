package controllers;

import beans.*;
import managers.LogManager;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.apache.spark.sql.functions.col;

// Specifying create table column data types on write
//jdbcDF.write
//        .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
//        .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

public class FLCacheController {
    private CacheSystemController c;

    public FLCacheController(CacheSystemController c) {
        this.c = c;
    }

    public void updateCache(SensorBean sensorBean, GranularityBean requiredGranularity, ArrayList<TimeRangeBean> nonExistingDataRanges) {
        LogManager.logInfo("[Updating cache databases][NonExistingDataRanges:" + nonExistingDataRanges + "]");
        FLCacheTableBean flCacheTableBean = sensorBean.getFlCacheTableBean();
        for (TimeRangeBean timeRangeBean : nonExistingDataRanges) {
            List<Pair<TimeRangeBean, SLCacheTableBean>> intersectionTimeRangeVsSLCacheTables = c.sensorController.getIntersectionTimeRangeVsSLCacheTables(sensorBean, timeRangeBean);
            LogManager.logPriorityInfo("intersectionTimeRange:" + intersectionTimeRangeVsSLCacheTables);
            for (Pair<TimeRangeBean, SLCacheTableBean> slCacheTableBeanPair : intersectionTimeRangeVsSLCacheTables) {
                //fetch this timeRange from this slcachetable
                ArrayList<TimeRangeBean> slNonExistingTimeRangeBeans = new ArrayList<>();
                slNonExistingTimeRangeBeans.add(slCacheTableBeanPair.getLeft());
                for (GranularityBean currentGranularity = requiredGranularity; currentGranularity != null; currentGranularity = c.granularityController.nextSmallerGranularity(currentGranularity)) {
                    LogManager.logPriorityInfo("Checking granularity: " + currentGranularity.getGranularityId());
                    ArrayList<TimeRangeBean> newTimeRangeBeans = new ArrayList<>();
                    for (TimeRangeBean currentTimeRangeBean : slNonExistingTimeRangeBeans) {
                        //aggregate and update flCacheTable & slCacheTable
                        ArrayList<TimeRangeBean> existingDataRanges = c.bitmapController.getExistingDataRange(sensorBean.getSlBitmapBean(), currentGranularity, currentTimeRangeBean);
                        LogManager.logPriorityInfo("ExistingDataRange:" + existingDataRanges);
                        ArrayList<TimeRangeBean> nonExistingDataRange = c.bitmapController.getNonExistingDataRange(sensorBean.getSlBitmapBean(), currentGranularity, currentTimeRangeBean);
                        LogManager.logPriorityInfo("NonExistingDataRange:" + nonExistingDataRange);
                        for (TimeRangeBean existingTimeRangeBean : existingDataRanges) {
                            LogManager.logPriorityInfo("Data found in SL" + existingTimeRangeBean);
                            updateSLAndFLCacheFromSL(sensorBean, requiredGranularity, slCacheTableBeanPair.getRight(), flCacheTableBean, currentGranularity, existingTimeRangeBean);
                        }
                        newTimeRangeBeans.addAll(c.bitmapController.getNonExistingDataRange(sensorBean.getSlBitmapBean(), currentGranularity, currentTimeRangeBean));
                    }
                    slNonExistingTimeRangeBeans = newTimeRangeBeans;
                }

                for (TimeRangeBean currentTimeRangeBean : slNonExistingTimeRangeBeans) {
                    if (sensorBean instanceof SensorGroupBean) {
                        updateSLAndFLCacheFromSourceSensorGroup((SensorGroupBean) sensorBean, requiredGranularity, slCacheTableBeanPair.getRight(), flCacheTableBean, currentTimeRangeBean);
                    }else {
                        updateSLAndFLCacheFromSource(sensorBean, requiredGranularity, slCacheTableBeanPair.getRight(), flCacheTableBean, currentTimeRangeBean);
                    }
                }
            }
        }
    }

    private void updateSLAndFLCacheFromSourceSensorGroup(SensorGroupBean sensorGroupBean, GranularityBean requiredGranularity, SLCacheTableBean sl, FLCacheTableBean fl, TimeRangeBean timeRangeBean) {
        LogManager.logPriorityInfo("[updateSLAndFLCacheFromSourceSensorGroup]");
        GranularityBean smallerGranularity = c.granularityController.nextSmallerGranularity(requiredGranularity);
        Dataset<Row> sourceDataset = null;
        SourceTableBean commonSourceTableSchema=null;
        for (SensorBean sensorBean : sensorGroupBean.getSensorList()) {
            LogManager.logPriorityInfo(sensorBean.toString());
            List<Pair<TimeRangeBean, SourceTableBean>> intersectionTimeRangeVsSourceTables = c.sensorController.getIntersectionTimeRangeVsSourceTables(sensorBean, timeRangeBean);
            for (Pair<TimeRangeBean, SourceTableBean> sourceTableBeanPair : intersectionTimeRangeVsSourceTables) {
                TimeRangeBean currentTimeRangeBean = sourceTableBeanPair.getLeft();
                SourceTableBean sourceTable = sourceTableBeanPair.getRight();
                if(commonSourceTableSchema==null)commonSourceTableSchema=sourceTable;
                assert commonSourceTableSchema.getSchemaType()==sourceTable.getSchemaType();
                LogManager.logInfo("--[From SourceTable][Aggregation Started]: " + currentTimeRangeBean);
                currentTimeRangeBean.startTime = currentTimeRangeBean.startTime - (currentTimeRangeBean.startTime % requiredGranularity.getGranularityInTermsOfSeconds());
                currentTimeRangeBean.endTime = currentTimeRangeBean.endTime + (requiredGranularity.getGranularityInTermsOfSeconds() - (currentTimeRangeBean.endTime % requiredGranularity.getGranularityInTermsOfSeconds()));
                long i = currentTimeRangeBean.startTime;
                while (i < currentTimeRangeBean.endTime) {
                    long startTime = i;
                    long endTime = Math.min(startTime + requiredGranularity.getFetchIntervalAtOnceInSeconds(), currentTimeRangeBean.endTime);
                    LogManager.logInfo("----[From SourceTable][" + new Date(startTime * 1000) + "] to [" + new Date(endTime * 1000) + "]");
                    Dataset<Row> sensorDataset = c.sparkSession.read()
                            .jdbc(c.databaseController.getURL(sourceTable.getDatabaseBean()), sourceTable.getTableName(), sourceTable.getTsColumnName(), startTime, endTime, requiredGranularity.getNumPartitionsForEachInterval(), c.databaseController.getProperties(sourceTable.getDatabaseBean()));
                    Column tsFilter = col(sourceTable.getTsColumnName()).$greater$eq(startTime).and(col(sourceTable.getTsColumnName()).$less(endTime));
                    sensorDataset = sensorDataset.filter(col(sourceTable.getSensorIdColumnName()).equalTo(sensorBean.getSensorId())
                            .and(tsFilter));
                    sensorDataset = sensorDataset.withColumn(commonSourceTableSchema.getTsColumnName(),col(sourceTable.getTsColumnName()))
                            .withColumn(commonSourceTableSchema.getSensorIdColumnName(),col(sourceTable.getSensorIdColumnName()));
                    if (sourceDataset == null) sourceDataset = sensorDataset;
                    else sourceDataset = sourceDataset.union(sensorDataset);
                    i = endTime;
                }
                LogManager.logInfo("--[From SourceTable][Aggregation Finished]: " + currentTimeRangeBean);
            }
        }

        sourceDataset= c.aggregationManager.aggregateSpatially(sourceDataset,sensorGroupBean,commonSourceTableSchema);

        //update fl and sl after aggregation
        Dataset<Row> aggregatedRequiredGranularity;
        //if smaller granularity available fetch smaller granularity and required granularity from source if not then fetch required granularity only
        if (smallerGranularity != null) {
            Dataset<Row> aggregatedSmallerGranularity = c.aggregationManager.aggregateFromSource(sourceDataset, smallerGranularity, commonSourceTableSchema);
            aggregatedSmallerGranularity = aggregatedSmallerGranularity
                    .withColumn(sl.getTsColumnName(), col(commonSourceTableSchema.getTsColumnName()))
                    .withColumn(sl.getSensorIdColumnName(), col(commonSourceTableSchema.getSensorIdColumnName()));
            aggregatedSmallerGranularity = aggregatedSmallerGranularity.cache();
            aggregatedSmallerGranularity.write().mode(SaveMode.Append).jdbc(c.databaseController.getURL(sl.getDatabaseBean()), sl.getTableName(), c.databaseController.getProperties(sl.getDatabaseBean()));
            aggregatedRequiredGranularity = c.aggregationManager.aggregateFromSL(aggregatedSmallerGranularity, requiredGranularity, sl);
            aggregatedSmallerGranularity.unpersist();
        } else {
            aggregatedRequiredGranularity = c.aggregationManager.aggregateFromSource(sourceDataset, requiredGranularity, commonSourceTableSchema);
        }
        aggregatedRequiredGranularity = aggregatedRequiredGranularity.cache();
        aggregatedRequiredGranularity
                .withColumn(sl.getTsColumnName(), col(commonSourceTableSchema.getTsColumnName()))
                .withColumn(sl.getSensorIdColumnName(), col(commonSourceTableSchema.getSensorIdColumnName()))
                .write().mode(SaveMode.Append).jdbc(c.databaseController.getURL(sl.getDatabaseBean()), sl.getTableName(), c.databaseController.getProperties(sl.getDatabaseBean()));
        aggregatedRequiredGranularity
                .withColumn(fl.getTsColumnName(), col(commonSourceTableSchema.getTsColumnName()))
                .withColumn(fl.getSensorIdColumnName(), col(commonSourceTableSchema.getSensorIdColumnName()))
                .write().mode(SaveMode.Append).jdbc(c.databaseController.getURL(fl.getDatabaseBean()), fl.getTableName(), c.databaseController.getProperties(fl.getDatabaseBean()));
        aggregatedRequiredGranularity.unpersist();
        if (smallerGranularity != null) {
            //update bitmap of SL if smaller granularity available
            LogManager.logPriorityInfo("[updateSLAndFLCacheFromSourceSensorGroup]Bitmap update for " + smallerGranularity.getGranularityId() + " in " + timeRangeBean);
            c.bitmapController.updateBitmap(sensorGroupBean.getSlBitmapBean(), smallerGranularity, timeRangeBean);
            assert c.bitmapController.getNonExistingDataRange(sensorGroupBean.getSlBitmapBean(), smallerGranularity, timeRangeBean).isEmpty();
        }

    }

    private void updateSLAndFLCacheFromSource(SensorBean sensorBean, GranularityBean requiredGranularity, SLCacheTableBean sl, FLCacheTableBean fl, TimeRangeBean timeRangeBean) {
        LogManager.logPriorityInfo("[updateSLAndFLCacheFromSource]");
        GranularityBean smallerGranularity = c.granularityController.nextSmallerGranularity(requiredGranularity);
        List<Pair<TimeRangeBean, SourceTableBean>> intersectionTimeRangeVsSourceTables = c.sensorController.getIntersectionTimeRangeVsSourceTables(sensorBean, timeRangeBean);
        for (Pair<TimeRangeBean, SourceTableBean> sourceTableBeanPair : intersectionTimeRangeVsSourceTables) {
            TimeRangeBean currentTimeRangeBean = sourceTableBeanPair.getLeft();
            SourceTableBean sourceTable = sourceTableBeanPair.getRight();
            LogManager.logInfo("--[From SourceTable][Aggregation Started]: " + currentTimeRangeBean);
            currentTimeRangeBean.startTime = currentTimeRangeBean.startTime - (currentTimeRangeBean.startTime % requiredGranularity.getGranularityInTermsOfSeconds());
            currentTimeRangeBean.endTime = currentTimeRangeBean.endTime + (requiredGranularity.getGranularityInTermsOfSeconds() - (currentTimeRangeBean.endTime % requiredGranularity.getGranularityInTermsOfSeconds()));
            long i = currentTimeRangeBean.startTime;
            while (i < currentTimeRangeBean.endTime) {
                long startTime = i;
                long endTime = Math.min(startTime + requiredGranularity.getFetchIntervalAtOnceInSeconds(), currentTimeRangeBean.endTime);
                LogManager.logInfo("----[From SourceTable][" + new Date(startTime * 1000) + "] to [" + new Date(endTime * 1000) + "]");
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
                    aggregatedSmallerGranularity.unpersist();
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
                aggregatedRequiredGranularity.unpersist();
                i = endTime;
            }
            LogManager.logInfo("--[From SourceTable][Aggregation Finished]: " + currentTimeRangeBean);
        }

        if (smallerGranularity != null) {
            //update bitmap of SL if smaller granularity available
            LogManager.logPriorityInfo("[updateSLAndFLCacheFromSource]Bitmap update for " + smallerGranularity.getGranularityId() + " in " + timeRangeBean);
            c.bitmapController.updateBitmap(sensorBean.getSlBitmapBean(), smallerGranularity, timeRangeBean);
            assert c.bitmapController.getNonExistingDataRange(sensorBean.getSlBitmapBean(), smallerGranularity, timeRangeBean).isEmpty();
        }
    }

    private void updateSLAndFLCacheFromSL(SensorBean sensorBean, GranularityBean requiredGranularity, SLCacheTableBean sl, FLCacheTableBean fl, GranularityBean currentGranularity, TimeRangeBean currentTimeRangeBean) {
        LogManager.logPriorityInfo("[updateSLAndFLCacheFromSL]");
        LogManager.logInfo("--[From SLCache][Aggregation Started]: " + currentTimeRangeBean);
        currentTimeRangeBean.startTime = currentTimeRangeBean.startTime - (currentTimeRangeBean.startTime % requiredGranularity.getGranularityInTermsOfSeconds());
        currentTimeRangeBean.endTime = currentTimeRangeBean.endTime + (requiredGranularity.getGranularityInTermsOfSeconds() - (currentTimeRangeBean.endTime % requiredGranularity.getGranularityInTermsOfSeconds()));
        long i = currentTimeRangeBean.startTime;
        while (i < currentTimeRangeBean.endTime) {
            long startTime = i;
            long endTime = Math.min(startTime + requiredGranularity.getFetchIntervalAtOnceInSeconds(), currentTimeRangeBean.endTime);
            LogManager.logInfo("----[From SLCache][" + new Date(startTime * 1000) + "] to [" + new Date(endTime * 1000) + "]");
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
                aggregated.unpersist();
            }
            i = endTime;
        }
        LogManager.logInfo("--[From SLCache][Aggregation Finished]: " + currentTimeRangeBean);
    }

    public void cleanCache(SensorBean sensorBean, GranularityBean specifiedGranularity, TimeRangeBean timeRange) {
        FLCacheTableBean flCacheTableBean = sensorBean.getFlCacheTableBean();
        Connection connection=null;
        try {
            connection = DriverManager.getConnection(c.databaseController.getURL(flCacheTableBean.getDatabaseBean()), c.databaseController.getProperties(flCacheTableBean.getDatabaseBean()));
            PreparedStatement ps = connection.prepareStatement("DELETE from `"+flCacheTableBean.getTableName()+"` where `"+flCacheTableBean.getSensorIdColumnName()+"`=? and `granularityId`=? and `"+flCacheTableBean.getTsColumnName()+"`>=? and `"+flCacheTableBean.getTsColumnName()+"`<?");
            ps.setString(1,sensorBean.getSensorId());
            ps.setString(2,specifiedGranularity.getGranularityId());
            ps.setLong(3,timeRange.startTime);
            ps.setLong(4,timeRange.endTime);
            ps.executeLargeUpdate();
            ps.close();
            connection.close();
        } catch (SQLException e) {
            LogManager.logError("[" + this.getClass() + "][cleanCache][" + sensorBean + "]["+specifiedGranularity+"]" + e.getMessage());
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
