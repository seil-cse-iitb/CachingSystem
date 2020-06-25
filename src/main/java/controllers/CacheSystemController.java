package controllers;

import beans.*;
import managers.AggregationManager;
import managers.LogManager;
import managers.QueryLogManager;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/*
 * MQTTBean
 * SensorLiveStreamBean
 *
 * */

public class CacheSystemController {
    //TODO cache replacements policy

    public final SparkSession sparkSession;
    public final ConfigurationBean cb;
    public final Map<GranularityBean, Integer> granularityExecutingMap = new HashMap<>();
    public QueryController queryController;
    public GranularityController granularityController;
    public BitmapController bitmapController;
    public FLCacheController flCacheController;
    public SLCacheController slCacheController;
    public DatabaseController databaseController;
    public SensorController sensorController;
    public TimeRangeController timeRangeController;
    public LiveStreamController liveStreamController;
    public AggregationManager aggregationManager;
    public QueryLogManager queryLogManager;
    public LogManager logManager;
    public final Map<SensorBean, List<TimeRangeBean>> executingList = new HashMap<>();
    private final Map<SensorBean, LiveStreamBean> liveStreamingList = new HashMap<>();

    public CacheSystemController(SparkSession sparkSession, ConfigurationBean cb) {
        this.sparkSession = sparkSession;
        this.cb = cb;
        bitmapController = new BitmapController(this);
        queryController = new QueryController(this);
        flCacheController = new FLCacheController(this);
        slCacheController = new SLCacheController(this);
        granularityController = new GranularityController(this);
        databaseController = new DatabaseController(this);
        sensorController = new SensorController(this);
        timeRangeController = new TimeRangeController(this);
        liveStreamController = new LiveStreamController(this);
        aggregationManager = new AggregationManager(this);
        queryLogManager = new QueryLogManager(this);
        logManager = new LogManager();
    }

    public void start() {
        sparkSession.sparkContext().setLocalProperty("spark.scheduler.pool", "mainThread");
        LogManager.logCacheInit();
        //start query log cleanup thread
        queryLogManager.startQueryLogCleanupThread();
        //save granularities in db
//        granularityController.saveGranularities(sparkSession);
        //initialize bitmaps if not exists
        bitmapController.initBitmapsIfNotExists();
        //initialize execution_list table in fl cache
        flCacheController.initExecutionListIfNotExists();

        this.addAllSensorsToExecutingList();
        this.addAllGranularityToExecutingMap();
        LogManager.logPriorityInfo("[CachingSystem is ready to work!!!]");

        while (true) {
            //poll query log and get new queries
            List<QueryBean> newQueries = queryLogManager.getNewQueries();

            //currently assuming a query is about only a single sensor
            //start a thread for each query with their bitmaps
            for (QueryBean queryBean : newQueries) {
                try {
                    if (queryController.isLiveStreamQuery(queryBean)) {
                        synchronized (liveStreamingList) {
                            if (!changedStreamStatus(queryBean)) {
                                LogManager.logDebugInfo("skip ho gyi re query :(");
                                continue;
                            }
                            if (queryController.getLiveStreamDesiredStatus(queryBean) == LiveStreamController.START) {
                                //add sensor into liveStreaming list.
                                for (SensorBean sensorBean : queryBean.getSensorTimeRangeListMap().keySet()) {
                                    LogManager.logPriorityInfo("Starting live stream: " + sensorBean);
                                    LiveStreamBean liveStreamBean = new LiveStreamBean(sensorBean);
                                    liveStreamingList.put(sensorBean, liveStreamBean);
                                    for (String granularityString : cb.granularityBeanMap.keySet()) {
                                        startLiveStreamThread(liveStreamBean, cb.granularityBeanMap.get(granularityString));
                                    }
                                }
                            } else if (queryController.getLiveStreamDesiredStatus(queryBean) == LiveStreamController.END) {
                                //TODO shutdown the thread, and remove sensor from liveStreaming list
                                for (SensorBean sensorBean : queryBean.getSensorTimeRangeListMap().keySet()) {
                                    LogManager.logPriorityInfo("Ending live stream: " + sensorBean);
                                    liveStreamController.stopStream(liveStreamingList.get(sensorBean));
                                    liveStreamingList.remove(sensorBean);
                                }
                            }
                        }
                    } else {
                        //minimize the queries by minimizing overlapping of sensor_ids and timeranges
                        synchronized (executingList) {
                            LogManager.logDebugInfo("Should Execute Query: "+queryBean);
                            try {
                                if (!shouldExecute(queryBean))
                                    continue;
                            }catch(Exception e){
                                LogManager.logError("Should Execute Error: "+e.getLocalizedMessage());
                            }
                            LogManager.logInfo("Executing query:" + queryBean);
                            //add sensor timeranges in a global list
                            for (SensorBean sensorBean : queryBean.getSensorTimeRangeListMap().keySet()) {
                                List<TimeRangeBean> timeRangeBeans = queryBean.getSensorTimeRangeListMap().get(sensorBean);
                                assert timeRangeBeans.isEmpty() || executingList.get(sensorBean).addAll(timeRangeBeans);
                                LogManager.logPriorityInfo("added all timeranges");
                            }
                        }//TODO reduce time ranges based on if live stream is started or not. if live stream is started then for that sensor, time range should not exceed the startTime of its live stream
                        Thread thread = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                for (SensorBean sensorBean : queryBean.getSensorTimeRangeListMap().keySet()) {
                                    bitmapController.loadBitmaps(sensorBean);
                                }
                                //TODO load bitmap if it is first time. i.e. bitmap is null. Else it is already at the latest state because all the changes are done locally first then sent to db and no external code is changing the bitmap.
                                //TODO here we load bitmap for every query. Now what if a query is already executing for some different time range and now you replace the bitmap with new bitmap from db. (between the bitmap updated locally and in the db if this function fetches new bitmap then previous local updates will be lost.)
                                handleQuery(queryBean);
                                //remove sensor's timeranges from global list
                                synchronized (executingList) {
                                    for (SensorBean sensorBean : queryBean.getSensorTimeRangeListMap().keySet()) {
                                        List<TimeRangeBean> timeRangeBeans = queryBean.getSensorTimeRangeListMap().get(sensorBean);
                                        assert timeRangeBeans.isEmpty() || executingList.get(sensorBean).removeAll(timeRangeBeans);
                                        LogManager.logPriorityInfo("removed all timeranges");
                                    }
                                }
                            }
                        });
                        String threadName = "";
                        for (SensorBean sensorBean : queryBean.getSensorTimeRangeListMap().keySet())
                            threadName += "(" + sensorBean + ")";
                        thread.setName(threadName);
                        thread.start();
                    }
                } catch (Exception e) {
                    LogManager.logError("[Main Thread][Exception: " + e.getMessage() + "]");
                }
            }

            //**join all threads
            try {
                Thread.sleep(cb.queryPollingDurationInSeconds * 1000);
            } catch (InterruptedException e) {
                LogManager.logError("[" + this.getClass() + "]" + e.getMessage());
            }
        }
    }

    private Thread startLiveStreamThread(LiveStreamBean liveStreamBean, GranularityBean granularityBean) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                liveStreamController.startStream(liveStreamBean, granularityBean);
            }
        });
        String threadName = "(LiveStream:" + liveStreamBean.getSensorBean() + ")";
        thread.setName(threadName);
        liveStreamBean.setLiveStreamThread(thread);
        thread.start();
        return thread;
    }

    private boolean changedStreamStatus(QueryBean queryBean) {
        boolean desiredStreamState = queryController.getLiveStreamDesiredStatus(queryBean);
        for (SensorBean sensorBean : queryBean.getSensorTimeRangeListMap().keySet()) {
            LogManager.logDebugInfo("[DesiredStreamStatus: " + desiredStreamState + " and CurrentStreamStatus: " + liveStreamingList.containsKey(sensorBean) + "]");
            if (desiredStreamState == liveStreamingList.containsKey(sensorBean)) {
                return false;
            }
        }
        return true;
    }

    private void addAllGranularityToExecutingMap() {
        for (GranularityBean granularityBean : cb.granularityBeanMap.values()) {
            this.granularityExecutingMap.put(granularityBean, 0);
        }
    }

    private void addAllSensorsToExecutingList() {
        for (SensorBean sensorBean : cb.sensorBeanMap.values()) {
            this.executingList.put(sensorBean, new ArrayList<>());
        }
    }

    private boolean shouldExecute(QueryBean queryBean) {
        List<SensorBean> faultySensorBean = new ArrayList<>();
        for (SensorBean sensorBean : queryBean.getSensorTimeRangeListMap().keySet()) {
            boolean isSensorOk = true;
            if(!executingList.containsKey(sensorBean)) executingList.put(sensorBean,new ArrayList<>());
            if (executingList.get(sensorBean).size() > 0) {
                List<TimeRangeBean> timeRangeBeans = queryBean.getSensorTimeRangeListMap().get(sensorBean);
                List<TimeRangeBean> executingTimeRangeBeans = executingList.get(sensorBean);
                executingTimeRangeBeans.sort(Comparator.comparingLong(o -> o.startTime));
                for (int i = 0; i < timeRangeBeans.size(); i++) {
                    TimeRangeBean n = timeRangeBeans.get(i);
                    for (TimeRangeBean e : executingTimeRangeBeans) {
                        if (n.startTime < e.startTime && n.endTime <= e.startTime) {
//                                n |----------|
//                                                 e |----------|
                            break;
                        } else if (n.startTime < e.startTime && n.endTime <= e.endTime) {
                            n.endTime = e.startTime;
                            break;
//                                n |-----------|
//                                       e |-----------|
                        } else if (n.startTime < e.startTime && n.endTime > e.endTime) {
                            //add the extra timerange as a new timerange object
                            TimeRangeBean extra = new TimeRangeBean(e.endTime, n.endTime);
                            timeRangeBeans.add(extra);
                            LogManager.logPriorityInfo("added:" + extra);
                            n.endTime = e.startTime;
                            break;
//                                n |-------------------------|
//                                       e |-----------|
                        } else if (n.startTime >= e.startTime && n.endTime <= e.endTime) {
//                                          n |----|
//                                       e |-----------|
                            //remove because already executing
                            timeRangeBeans.remove(i);
                            i--;
                            LogManager.logPriorityInfo("removed:" + n);
                            break;
                        } else if (n.startTime >= e.startTime && n.endTime > e.endTime && n.startTime < e.endTime) {
                            n.startTime = e.endTime;
//                                             n |-----------|
//                                       e |-----------|
                        } else if (n.startTime >= e.endTime && n.endTime > e.endTime && n.startTime >= e.endTime) {
//                                                        n |-----------|
//                                       e |-----------|
                        } else {
                            LogManager.logPriorityInfo("above conditions didnt meet!!");
                        }
                    }
                }
                if (timeRangeBeans.isEmpty()) {
                    isSensorOk = false;
                }
            } else {
                isSensorOk = true;
            }
            if (!isSensorOk) {
                faultySensorBean.add(sensorBean);
            }
        }
        for (SensorBean sensorBean : faultySensorBean) {
            queryBean.getSensorTimeRangeListMap().remove(sensorBean);
        }
        return queryBean.getSensorTimeRangeListMap().keySet().size() > 0;
    }

    public void handleQuery(QueryBean query) {
        LogManager.logInfo("[Handling Query][" + query.getQueryStr() + "]");
        if (queryController.isCleanCacheQuery(query)) {
            cleanCache(query);
            return;
        }
        Map<SensorBean, List<TimeRangeBean>> sensorTimeRangeMap = query.getSensorTimeRangeListMap();
        for (SensorBean sensorBean : sensorTimeRangeMap.keySet()) {
            List<TimeRangeBean> timeRanges = sensorTimeRangeMap.get(sensorBean);
            GranularityBean granularity;
            if (queryController.isGranularitySpecified(query)) {
                granularity = queryController.getSpecifiedGranularity(query);
                granularity = granularity == null ? granularityController.eligibleGranularity(timeRanges) : granularity;
            } else {
                granularity = granularityController.eligibleGranularity(timeRanges);
            }
            String poolName = "queryExecutingThreads(" + granularity.getGranularityId() + ")";
            synchronized (this.granularityExecutingMap) {
                Integer count = this.granularityExecutingMap.get(granularity);
                this.granularityExecutingMap.put(granularity, count + 1);
                poolName += "(" + ((count + 1) % granularity.getNumParallelQuery()) + ")";
            }
            sparkSession.sparkContext().setLocalProperty("spark.scheduler.pool", poolName);

            LogManager.logPriorityInfo("[Eligible granularity:" + granularity.getGranularityId() + "]");
            LogManager.logPriorityInfo("[TimeRanges required:" + timeRanges + "]");
            ArrayList<TimeRangeBean> nonExistingDataRanges = new ArrayList<>();
            for (TimeRangeBean timeRange : timeRanges) {
                timeRange.startTime = timeRange.startTime - (timeRange.startTime % granularity.getGranularityInTermsOfSeconds());
                timeRange.endTime = timeRange.endTime - (timeRange.endTime % granularity.getGranularityInTermsOfSeconds());
                nonExistingDataRanges.addAll(bitmapController.getNonExistingDataRange(sensorBean.getFlBitmapBean(), granularity, timeRange));
            }
            if (nonExistingDataRanges.size() > 0) {
                try {
                    flCacheController.addToExecutionList(sensorBean, granularity, nonExistingDataRanges);
                    flCacheController.updateCache(sensorBean, granularity, nonExistingDataRanges);
                    bitmapController.updateBitmap(sensorBean.getFlBitmapBean(), granularity, nonExistingDataRanges);
                    bitmapController.updateBitmap(sensorBean.getSlBitmapBean(), granularity, nonExistingDataRanges);
                } catch (Exception e) {
                    LogManager.logError("[" + this.getClass() + "][" + query + "]" + e.getMessage());
                    LogManager.logPriorityInfo("[Cleaning Cache][Sensor:" + sensorBean + "][Granularity:" + granularity.getGranularityId() + "][" + nonExistingDataRanges + "]");
                    for (TimeRangeBean timeRange : nonExistingDataRanges) {
                        flCacheController.cleanCache(sensorBean, granularity, timeRange);
                        slCacheController.cleanCache(sensorBean, granularity, timeRange);
                        bitmapController.cleanBitmap(sensorBean.getFlBitmapBean(), granularity, timeRange);
                        bitmapController.cleanBitmap(sensorBean.getSlBitmapBean(), granularity, timeRange);
                    }
                } finally {
                    flCacheController.removeFromExecutionList(sensorBean, granularity, nonExistingDataRanges);
                }
            } else {
                LogManager.logInfo("[Complete data exists of this query]");
            }
            bitmapController.saveBitmaps(sensorBean);
            synchronized (this.granularityExecutingMap) {
                Integer count = this.granularityExecutingMap.get(granularity);
                this.granularityExecutingMap.put(granularity, count - 1);
            }
        }
    }

    private void cleanCache(QueryBean query) {
        LogManager.logPriorityInfo("[Cleaning Cache]" + Collections.singletonList(query.getSensorTimeRangeListMap()));
        GranularityBean specifiedGranularity = queryController.getSpecifiedGranularity(query);
        Map<SensorBean, List<TimeRangeBean>> sensorTimeRangeMap = query.getSensorTimeRangeListMap();
        for (SensorBean sensorBean : sensorTimeRangeMap.keySet()) {
            List<TimeRangeBean> timeRanges = sensorTimeRangeMap.get(sensorBean);
            for (TimeRangeBean timeRange : timeRanges) {
                if (specifiedGranularity != null) {
                    flCacheController.cleanCache(sensorBean, specifiedGranularity, timeRange);
                    slCacheController.cleanCache(sensorBean, specifiedGranularity, timeRange);
                    bitmapController.cleanBitmap(sensorBean.getFlBitmapBean(), specifiedGranularity, timeRange);
                    bitmapController.cleanBitmap(sensorBean.getSlBitmapBean(), specifiedGranularity, timeRange);
                } else {
                    for (GranularityBean granularityBean :
                            cb.granularityBeanMap.values()) {
                        flCacheController.cleanCache(sensorBean, granularityBean, timeRange);
                        slCacheController.cleanCache(sensorBean, granularityBean, timeRange);
                        bitmapController.cleanBitmap(sensorBean.getFlBitmapBean(), granularityBean, timeRange);
                        bitmapController.cleanBitmap(sensorBean.getSlBitmapBean(), granularityBean, timeRange);
                    }
                }
            }
            bitmapController.saveBitmaps(sensorBean);
        }

    }


    private void clearCacheAndBitmapTables() {
        for (FLCacheTableBean flc : cb.flCacheTableBeanMap.values()) {
            Connection connection = null;
            try {
                connection = DriverManager.getConnection(databaseController.getURL(flc.getDatabaseBean()), databaseController.getProperties(flc.getDatabaseBean()));
                String tableName = flc.getTableName() + "_" + cb.bitmapTableNameSuffix;
                PreparedStatement preparedStatement = connection.prepareStatement(String.format("truncate %s", tableName));
                preparedStatement.executeUpdate();
                preparedStatement.close();

                tableName = flc.getTableName();
                preparedStatement = connection.prepareStatement(String.format("truncate %s", tableName));
                preparedStatement.executeUpdate();
                preparedStatement.close();

                connection.close();
            } catch (SQLException e) {
                LogManager.logError("[" + this.getClass() + "][clearCacheAndBitmapTables]" + flc + e.getMessage());
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        LogManager.logError("[" + this.getClass() + "][connection closing exception]" + e.getMessage());
                    }
                }
            }
        }
        for (SLCacheTableBean slc : cb.slCacheTableBeanMap.values()) {
            Connection connection = null;
            try {
                connection = DriverManager.getConnection(databaseController.getURL(slc.getDatabaseBean()), databaseController.getProperties(slc.getDatabaseBean()));
                String tableName = slc.getTableName();
                PreparedStatement preparedStatement = connection.prepareStatement(String.format("truncate %s", tableName));
                preparedStatement.executeUpdate();
                preparedStatement.close();

                connection.close();
            } catch (SQLException e) {
                LogManager.logError("[" + this.getClass() + "][clearCacheAndBitmapTables]" + slc + e.getMessage());
            } finally {
                if (connection != null) {
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
