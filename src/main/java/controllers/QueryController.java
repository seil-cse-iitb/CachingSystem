package controllers;

import beans.*;
import managers.LogManager;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.execution.SparkSqlParser;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.unsafe.types.UTF8String;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.sort;

// var parsePlan = new SparkSqlParser(new SQLConf()).parsePlan("SELECT JSON_OBJECT('meta_data','specified_granularity','specified_granularity','%') as meta_data FROM power WHERE sensor_id ='$power_sensor' and granularity like '$granularity' order by time_sec");
public class QueryController {

    CacheSystemController c;

    public QueryController(CacheSystemController c) {
        this.c = c;
    }

    public List<QueryBean> preprocessQueries(List<QueryBean> queries) {
        for (QueryBean query : queries) {
            LogManager.logDebugInfo("[PreprocessingQueryBean]" + query);
            LogicalPlan parsePlan = new SparkSqlParser(new SQLConf()).parsePlan(query.getQueryStr());
            LogicalPlan filterPlan = getFilterPlan(parsePlan);
            LogicalPlan projectPlan = getProjectPlan(parsePlan);
            Seq<NamedExpression> namedExpressionSeq = ((Project) projectPlan).projectList();
            namedExpressionSeq.foreach(new AbstractFunction1<NamedExpression, Object>() {
                @Override
                public Object apply(NamedExpression v1) {
                    if(v1.name().equalsIgnoreCase("meta_data")){
                        String metaData = ((Expression)v1).children().head().simpleString();
                        try {
                            JSONObject metaDataJson= (JSONObject) new JSONParser().parse(metaData);
                            for (Object key:metaDataJson.keySet()) {
                                query.getMetaData().put((String)key,(String)metaDataJson.get(key));
                            }
                        } catch (ParseException e) {
                            LogManager.logError("[JSONParsing:"+e.getMessage()+"]putting meta_data=from_cache");
                            query.getMetaData().put("meta_data","from_cache");
                        }
                    }
                    return false;
                }
            });
            for(String key:query.getMetaData().keySet()){
                LogManager.logPriorityInfo(key+" : "+query.getMetaData().get(key));
            }
            Expression whereConditions = ((Filter) filterPlan).condition();
            String tableName = ((UnresolvedRelation) getUnresolvedRelation(filterPlan)).tableName();
            FLCacheTableBean flCacheTableBean = null;
            for (FLCacheTableBean flCacheTableBeanTemp : c.cb.flCacheTableBeanMap.values()) {
                if (ConfigurationBean.SchemaType.valueOf(tableName) == flCacheTableBeanTemp.getSchemaType()) {
                    flCacheTableBean = flCacheTableBeanTemp;
                    break;
                }
            }
            if (flCacheTableBean == null) {
                LogManager.logError("[Table not found:" + tableName + "]");
                continue;
            }
            ArrayList<Expression> tsConditions = new ArrayList<>();
            extractTSConditions(flCacheTableBean, whereConditions, null, tsConditions);
            ArrayList<Expression> sensorIdConditions = new ArrayList<>();
            extractSensorIdConditions(flCacheTableBean, whereConditions, null, sensorIdConditions);

            ArrayList<SensorBean> sensors = extractSensors(sensorIdConditions);
            for (SensorBean sensor : sensors) {
                ArrayList<TimeRangeBean> timeRanges = extractTimeRanges(tsConditions);//always create new list of timeranges, so that changes in timeRange.startTime and endTime does not affect other sensors of the query
                query.getSensorTimeRangeListMap().put(sensor, timeRanges);
            }

        }
        return queries;
    }

    private LogicalPlan getProjectPlan(LogicalPlan logicalPlan) {
        if (logicalPlan.nodeName().equals("Project")) return logicalPlan;
        else return getProjectPlan(logicalPlan.children().head());
    }

    private LogicalPlan getFilterPlan(LogicalPlan logicalPlan) {
        if (logicalPlan.nodeName().equals("Filter")) return logicalPlan;
        else return getFilterPlan(logicalPlan.children().head());
    }

    private LogicalPlan getUnresolvedRelation(LogicalPlan logicalPlan) {
        if (logicalPlan.nodeName().equals("UnresolvedRelation")) return logicalPlan;
        else return getUnresolvedRelation(logicalPlan.children().head());
    }

    private ArrayList<TimeRangeBean> extractTimeRanges(ArrayList<Expression> tsConditions) {
        //TODO Check logic and extend for more generic time ranges
        //Very bad code down there. Correct it when you are free.
        ArrayList<TimeRangeBean> timeRanges = new ArrayList<>();
        ArrayList<Long> startTimes = new ArrayList<>();
        ArrayList<Long> endTimes = new ArrayList<>();
        for (Expression tsExpr : tsConditions) {
            Long startTime = 0L, endTime = (long) Math.round(System.currentTimeMillis() / 1000);
            if (tsExpr instanceof GreaterThanOrEqual) {
                startTime = extractTS(tsExpr);
                startTimes.add(startTime);
            } else if (tsExpr instanceof GreaterThan) {
                startTime = extractTS(tsExpr) + 1; //Little bit wrong
                startTimes.add(startTime);
            } else if (tsExpr instanceof LessThanOrEqual) {
                endTime = extractTS(tsExpr) + 1; //Little bit wrong
                endTimes.add(endTime);
            } else if (tsExpr instanceof LessThan) {
                endTime = extractTS(tsExpr);
                endTimes.add(endTime);
            } else if (tsExpr instanceof EqualTo) {
                startTime = extractTS(tsExpr);
                startTimes.add(startTime);
                endTime = startTime + 1;
                endTimes.add(endTime);
            }
        }
        sort(startTimes);
        sort(endTimes);
        while (!startTimes.isEmpty() && !endTimes.isEmpty()) {

            if (startTimes.get(0) < endTimes.get(0)) {
                timeRanges.add(new TimeRangeBean(startTimes.get(0), endTimes.get(0)));
                startTimes.remove(0);
                endTimes.remove(0);
            } else {
                //TODO edit in previous TimeRange
                LogManager.logInfo("[Skipping query]Bad Time Ranges in Query");
                break;
            }

        }
        return timeRanges;
    }

    private Long extractTS(Expression tsExpr) {
        if (tsExpr.children().head() instanceof Literal) {
            return ((Integer) ((Literal) tsExpr.children().head()).value()).longValue();
        } else if (tsExpr.children().last() instanceof Literal) {
            return ((Integer) ((Literal) tsExpr.children().last()).value()).longValue();
        }
        return -1L;
    }

    private ArrayList<SensorBean> extractSensors(ArrayList<Expression> sensorIdConditions) {
        ArrayList<SensorBean> sensors = new ArrayList<>();
        for (Expression sensorIdCondition : sensorIdConditions) { //for now assumption is that there will be only one sensorIdCondition
            if (sensorIdCondition.children().head() instanceof Literal) {
                SensorBean sensorBean = c.cb.sensorBeanMap.get(((UTF8String) ((Literal) sensorIdCondition.children().head()).value()).toString());
                if (sensorBean != null)
                    sensors.add(sensorBean);
            } else if (sensorIdCondition.children().last() instanceof Literal) {
                SensorBean sensorBean = c.cb.sensorBeanMap.get(((UTF8String) ((Literal) sensorIdCondition.children().last()).value()).toString());
                if (sensorBean != null)
                    sensors.add(sensorBean);
            }
        }
        return sensors;
    }

    private void extractTSConditions(FLCacheTableBean flCacheTableBean, Expression curExpr, Expression parentExpr, List<Expression> tsConditions) {
        assert tsConditions != null;
        if (curExpr.nodeName().equalsIgnoreCase("UnresolvedAttribute")) {
            if (curExpr.flatArguments().hasNext()) {
                if (((String) curExpr.flatArguments().next()).equalsIgnoreCase(flCacheTableBean.getTsColumnName())) {
                    tsConditions.add(parentExpr);
                }
            }
        }
        Iterator<Expression> iterator = curExpr.children().iterator();
        while (iterator.hasNext()) {
            extractTSConditions(flCacheTableBean, iterator.next(), curExpr, tsConditions);
        }
    }

    private void extractSensorIdConditions(FLCacheTableBean flCacheTableBean, Expression curExpr, Expression parentExpr, List<Expression> sensorConditions) {
        assert sensorConditions != null;
        if (curExpr.nodeName().equalsIgnoreCase("UnresolvedAttribute")) {
            if (curExpr.flatArguments().hasNext()) {
                if (((String) curExpr.flatArguments().next()).equalsIgnoreCase(flCacheTableBean.getSensorIdColumnName())) {
                    sensorConditions.add(parentExpr);
                }
            }
        }
        Iterator<Expression> iterator = curExpr.children().iterator();
        while (iterator.hasNext()) {
            extractSensorIdConditions(flCacheTableBean, iterator.next(), curExpr, sensorConditions);
        }
    }

    public boolean isGranularitySpecified(QueryBean query) {
        return query.getMetaData().get("meta_data").equalsIgnoreCase("specified_granularity");
    }

    public GranularityBean getSpecifiedGranularity(QueryBean query) {
        for (String gstr:c.cb.granularityBeanMap.keySet()) {
         if(query.getMetaData().get("specified_granularity").equalsIgnoreCase(gstr)){
             return c.cb.granularityBeanMap.get(gstr);
         }
        }
        return null;
    }
}
