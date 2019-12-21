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
    public AggregationManager aggregationManager;
    public QueryLogManager queryLogManager;
    public LogManager logManager;
    public Map<SensorBean, List<TimeRangeBean>> executingList = new HashMap<>();

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
        aggregationManager = new AggregationManager(this);
        queryLogManager = new QueryLogManager(this);
        logManager = new LogManager();
    }

    public void start() {
        sparkSession.sparkContext().setLocalProperty("spark.scheduler.pool", "mainThread");
        logManager.logCacheInit();
        //start query log cleanup thread
        queryLogManager.startQueryLogCleanupThread();
        //save granularities in db
        granularityController.saveGranularities(sparkSession);
        //initialize bitmaps if not exists
        bitmapController.initBitmapsIfNotExists();

        this.addAllSensorsToExecutingList();
        this.addAllGranularityToExecutingMap();
        while (true) {
            //poll query log and get new queries
            List<QueryBean> newQueries = queryLogManager.getNewQueries();

            //currently assuming a query is about only a single sensor
            //start a thread for each query with their bitmaps
            for (QueryBean queryBean : newQueries) {
                //minimize the queries by minimizing overlapping of sensor_ids and timeranges
                synchronized (executingList) {
                    if (!shouldExecute(queryBean))
                        continue;
                    logManager.logInfo("Executing query:" + queryBean);
                    //add sensor timeranges in a global list
                    for (SensorBean sensorBean : queryBean.getSensorTimeRangeListMap().keySet()) {
                        List<TimeRangeBean> timeRangeBeans = queryBean.getSensorTimeRangeListMap().get(sensorBean);
                        assert executingList.get(sensorBean).addAll(timeRangeBeans);
                        logManager.logPriorityInfo("added all timeranges");
                    }
                }
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        for (SensorBean sensorBean : queryBean.getSensorTimeRangeListMap().keySet()) {
                            bitmapController.loadBitmaps(sensorBean);
                        }
                        handleQuery(queryBean);
                        //remove sensor's timeranges from global list
                        synchronized (executingList) {
                            for (SensorBean sensorBean : queryBean.getSensorTimeRangeListMap().keySet()) {
                                List<TimeRangeBean> timeRangeBeans = queryBean.getSensorTimeRangeListMap().get(sensorBean);
                                assert executingList.get(sensorBean).removeAll(timeRangeBeans);
                                logManager.logPriorityInfo("removed all timeranges");
                            }
                        }
                    }
                });
                String threadName = "";
                for (SensorBean sensorBean : queryBean.getSensorTimeRangeListMap().keySet())
                    threadName += "(" + sensorBean.getSensorId() + ")";
                thread.setName(threadName);
                thread.start();
            }

            //**join all threads
            try {
                Thread.sleep(cb.queryPollingDurationInSeconds * 1000);
            } catch (InterruptedException e) {
                logManager.logError("[" + this.getClass() + "]" + e.getMessage());
            }
        }
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
                            logManager.logPriorityInfo("added:" + extra);
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
                            logManager.logPriorityInfo("removed:" + n);
                            break;
                        } else if (n.startTime >= e.startTime && n.endTime > e.endTime && n.startTime < e.endTime) {
                            n.startTime = e.endTime;
//                                             n |-----------|
//                                       e |-----------|
                        } else if (n.startTime >= e.endTime && n.endTime > e.endTime && n.startTime >= e.endTime) {
//                                                        n |-----------|
//                                       e |-----------|
                        } else {
                            logManager.logPriorityInfo("above conditions didnt meet!!");
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
        if (queryBean.getSensorTimeRangeListMap().keySet().size() > 0)
            return true;
        else
            return false;
    }

    public void handleQuery(QueryBean query) {
        logManager.logInfo("[Handling Query][" + query.getQueryStr() + "]");
        Map<SensorBean, List<TimeRangeBean>> sensorTimeRangeMap = query.getSensorTimeRangeListMap();
        for (SensorBean sensorBean : sensorTimeRangeMap.keySet()) {
            List<TimeRangeBean> timeRanges = sensorTimeRangeMap.get(sensorBean);
            GranularityBean granularity;
            if(queryController.isGranularitySpecified(query)){
                granularity = queryController.getSpecifiedGranularity(query);
                granularity = granularity==null?granularityController.eligibleGranularity(timeRanges):granularity;
            }else {
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
                timeRange.endTime = timeRange.endTime + (granularity.getGranularityInTermsOfSeconds() - (timeRange.endTime % granularity.getGranularityInTermsOfSeconds()));
                nonExistingDataRanges.addAll(bitmapController.getNonExistingDataRange(sensorBean.getFlBitmapBean(), granularity, timeRange));
            }
            if (nonExistingDataRanges.size() > 0) {
                flCacheController.updateCache(sensorBean, granularity, nonExistingDataRanges);
                bitmapController.updateBitmap(sensorBean.getFlBitmapBean(), granularity, nonExistingDataRanges);
                bitmapController.updateBitmap(sensorBean.getSlBitmapBean(), granularity, nonExistingDataRanges);
            } else {
                LogManager.logInfo("[Complete data exists of this query]");
            }
            bitmapController.saveBitmaps(sensorBean);
            synchronized (this.granularityExecutingMap){
                Integer count = this.granularityExecutingMap.get(granularity);
                this.granularityExecutingMap.put(granularity, count - 1);
            }
        }
    }


    private void clearCacheAndBitmapTables() {
        for (FLCacheTableBean flc : cb.flCacheTableBeanMap.values()) {
            try {
                Connection connection = DriverManager.getConnection(databaseController.getURL(flc.getDatabaseBean()), databaseController.getProperties(flc.getDatabaseBean()));
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
                logManager.logError("[" + this.getClass() + "][clearCacheAndBitmapTables]" + flc + e.getMessage());
            }
        }
        for (SLCacheTableBean slc : cb.slCacheTableBeanMap.values()) {
            try {
                Connection connection = DriverManager.getConnection(databaseController.getURL(slc.getDatabaseBean()), databaseController.getProperties(slc.getDatabaseBean()));
                String tableName = slc.getTableName();
                PreparedStatement preparedStatement = connection.prepareStatement(String.format("truncate %s", tableName));
                preparedStatement.executeUpdate();
                preparedStatement.close();

                connection.close();
            } catch (SQLException e) {
                logManager.logError("[" + this.getClass() + "][clearCacheAndBitmapTables]" + slc + e.getMessage());
            }
        }
    }
}
