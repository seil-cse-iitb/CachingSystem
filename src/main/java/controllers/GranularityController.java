package controllers;

import beans.DatabaseBean;
import beans.FLCacheTableBean;
import beans.GranularityBean;
import beans.TimeRangeBean;
import managers.LogManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.*;

public class GranularityController {
    CacheSystemController c;
    private Comparator<GranularityBean> granularityComparator = Comparator.comparingInt(GranularityBean::getDisplayPriority);

    public GranularityController(CacheSystemController c) {
        this.c = c;
    }

    public void saveGranularities(SparkSession sparkSession) {
        LogManager.logPriorityInfo("[Updating granularities in FL Cache]");
        Set<DatabaseBean> databaseBeans = new HashSet<>();
        for (FLCacheTableBean flCacheTableBean : c.cb.flCacheTableBeanMap.values()) {
            databaseBeans.add(flCacheTableBean.getDatabaseBean());
        }
        for (DatabaseBean db : databaseBeans) {
            c.sparkSession.sparkContext().setLocalProperty("callSite.short", "Updating granularities in FL Cache");
            c.sparkSession.sparkContext().setLocalProperty("callSite.long", "Updating granularities in FL Cache in "+db);
            List<GranularityBean> granularityBeans = new ArrayList<>(c.cb.granularityBeanMap.values());
            Dataset<Row> granularityDataset = sparkSession.createDataFrame(granularityBeans, GranularityBean.class);
            granularityDataset.orderBy("displayPriority").write().mode(SaveMode.Overwrite).jdbc(c.databaseController.getURL(db), c.cb.granularityTableNameSuffix, c.databaseController.getProperties(db));
        }
        c.sparkSession.sparkContext().setLocalProperty("callSite.short", null);
        c.sparkSession.sparkContext().setLocalProperty("callSite.long", null);
    }

    public GranularityBean eligibleGranularity(List<TimeRangeBean> timeRanges) {
        long totalTime = 0;
        for (TimeRangeBean timeRange : timeRanges) {
            totalTime += timeRange.endTime - timeRange.startTime;
        }
        ArrayList<GranularityBean> granularityBeans = new ArrayList<>(c.cb.granularityBeanMap.values());
        granularityBeans.sort(granularityComparator);
        for (GranularityBean granularity : granularityBeans) {
            if (totalTime <= granularity.getDisplayLimitInSeconds()) {
                return granularity;
            }
        }
        return granularityBeans.get(granularityBeans.size() - 1);
    }

    public GranularityBean nextSmallerGranularity(GranularityBean granularityBean) {
        ArrayList<GranularityBean> granularityBeans = new ArrayList<>(c.cb.granularityBeanMap.values());
        granularityBeans.sort((a,b)->b.getDisplayLimitInSeconds() - a.getDisplayLimitInSeconds());
        for (GranularityBean bean : granularityBeans) {
            if (bean.getGranularityInTermsOfSeconds() < granularityBean.getGranularityInTermsOfSeconds()) {
                return bean;
            }
        }
        return null;
    }
}
