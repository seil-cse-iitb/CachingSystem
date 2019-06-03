package controllers;

import beans.*;
import managers.AggregationManager;
import managers.LogManager;
import managers.QueryLogManager;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CacheSystemController {

    public final SparkSession sparkSession;
    public final ConfigurationBean cb;
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

    public Map<QueryBean, Thread> executingQueries = new ConcurrentHashMap<>();
    public Map<SensorBean, List<TimeRangeBean>> executingSensorBeanVsTimeRangeListMap = new ConcurrentHashMap<>();

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

        while (true) {
            //poll query log and get new queries
            List<QueryBean> newQueries = queryLogManager.getNewQueries();

            //currently assuming a query is about only a single sensor
            //start a thread for each query with their bitmaps
            for (QueryBean queryBean : newQueries) {
                //minimize the queries by minimizing overlapping of sensor_ids and timeranges
                if (!shouldExecute(queryBean))
                    continue;
                logManager.logInfo("Executing query:" + queryBean);
                for (SensorBean sensorBean : queryBean.getSensorTimeRangeListMap().keySet()) {
                    if (executingSensorBeanVsTimeRangeListMap.containsKey(sensorBean)) {
                        executingSensorBeanVsTimeRangeListMap.get(sensorBean).addAll(queryBean.getSensorTimeRangeListMap().get(sensorBean));
                    } else {
                        executingSensorBeanVsTimeRangeListMap.put(sensorBean, queryBean.getSensorTimeRangeListMap().get(sensorBean));
                    }
                }
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        sparkSession.sparkContext().setLocalProperty("spark.scheduler.pool", "queryExecutingThreads");
                        for (SensorBean sensorBean : queryBean.getSensorTimeRangeListMap().keySet()) {
                            bitmapController.loadBitmaps(sensorBean);
                        }
                        handleQuery(queryBean);
                        for (SensorBean sensorBean : queryBean.getSensorTimeRangeListMap().keySet()) {
                            executingSensorBeanVsTimeRangeListMap.get(sensorBean).removeAll(queryBean.getSensorTimeRangeListMap().get(sensorBean));
                            if (executingSensorBeanVsTimeRangeListMap.get(sensorBean).isEmpty()) {
                                executingSensorBeanVsTimeRangeListMap.remove(sensorBean);
                            }
                        }
                        executingQueries.remove(queryBean);
                    }
                });
                String threadName = "";
                for (SensorBean sensorBean : queryBean.getSensorTimeRangeListMap().keySet())
                    threadName += "(" + sensorBean.getSensorId() + ")";
                thread.setName(threadName);
                thread.start();
                executingQueries.put(queryBean, thread);
            }

            //**join all threads
            try {
                Thread.sleep(cb.queryPollingDurationInSeconds * 1000);
            } catch (InterruptedException e) {
                logManager.logError("[" + this.getClass() + "]" + e.getMessage());
            }
        }
    }

    private boolean shouldExecute(QueryBean queryBean) {
        for (SensorBean sensorBean : queryBean.getSensorTimeRangeListMap().keySet()) {
            if (executingSensorBeanVsTimeRangeListMap.containsKey(sensorBean)) {
                List<TimeRangeBean> timeRangeBeans = queryBean.getSensorTimeRangeListMap().get(sensorBean);
                List<TimeRangeBean> executingTimeRangeBeans = executingSensorBeanVsTimeRangeListMap.get(sensorBean);
                executingTimeRangeBeans.sort(Comparator.comparingLong(o -> o.startTime));
                //subtract executingTimeRanges from timeRanges and remove timeRangeBeans which are already executing
                List<TimeRangeBean> newTimeRangeBeans = new ArrayList<>();
                assert timeRangeBeans.size()>0;
                assert executingTimeRangeBeans.size()>0;
                for (int i = 0; i < timeRangeBeans.size(); i++) {
                    TimeRangeBean n = timeRangeBeans.get(i);
                    for (TimeRangeBean e : executingTimeRangeBeans) {
                        if (n.startTime < e.startTime && n.endTime <= e.startTime) {
                            newTimeRangeBeans.add(n);
                        } else if (n.startTime < e.startTime && n.endTime <= e.endTime) {
                            n.endTime = e.startTime;
                            newTimeRangeBeans.add(n);
                        } else if (n.startTime < e.startTime && n.endTime > e.endTime) {
                            n.endTime = e.startTime;
                            newTimeRangeBeans.add(n);
                            timeRangeBeans.add(new TimeRangeBean(e.endTime, n.endTime));
                        } else if (n.startTime >= e.startTime && n.endTime <= e.endTime) {
                            //discard because already executing
                        } else if (n.startTime >= e.startTime && n.endTime > e.endTime) {
                            n.startTime = e.endTime;
                            newTimeRangeBeans.add(n);
                        } else if (n.startTime >= e.endTime && n.endTime > e.endTime) {
                            newTimeRangeBeans.add(n);
                        }
                    }
                    logManager.logPriorityInfo(i+"");
                }
                logManager.logPriorityInfo(newTimeRangeBeans.toString());
                if (newTimeRangeBeans.isEmpty()) {
                    return false;
                } else {
                    queryBean.getSensorTimeRangeListMap().put(sensorBean, newTimeRangeBeans);
                }
            }
        }
        return true;
    }

    public void handleQuery(QueryBean query) {
        logManager.logInfo("[Handling Query][" + query.getQueryStr() + "]");
        Map<SensorBean, List<TimeRangeBean>> sensorTimeRangeMap = query.getSensorTimeRangeListMap();
        for (SensorBean sensorBean : sensorTimeRangeMap.keySet()) {
            List<TimeRangeBean> timeRanges = sensorTimeRangeMap.get(sensorBean);
            GranularityBean granularity = granularityController.eligibleGranularity(timeRanges);
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
        }
    }

}
