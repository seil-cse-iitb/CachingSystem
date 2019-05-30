package version1.manager;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SparkSqlParser;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.Iterator;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;

import static java.util.Collections.sort;

public class Query implements Serializable {


    public static MapFunction<Row, Query> queryMapFunction = new MapFunction<Row, Query>() {
        public Query call(Row value) throws Exception {
            Timestamp queryTimestamp = value.getTimestamp(value.fieldIndex("event_time"));
            String queryStr = value.getString(value.fieldIndex("argument"));
            String userHost = value.getAs("user_host");
            Query query = new Query(queryTimestamp, queryStr, userHost);
            return query;
        }
    };
    private Timestamp queryTimestamp;
    private String queryStr;
    private String userHost;
    private final ArrayList<Expression> tsConditions = new ArrayList<>();
    private final ArrayList<Expression> sensorIdConditions = new ArrayList<>();
    private final Map<Sensor, List<TimeRange>> sensorTimeRangeMap = new HashMap<>();


    public Query() {
    }

    public Query(Timestamp queryTimestamp, String queryStr, String userHost) {
        this.queryTimestamp = queryTimestamp;
        this.queryStr = queryStr;
        this.userHost = userHost;
    }

    public static List<Query> preprocessQueries(List<Query> queries) {
        for (Query query : queries) {
            LogManager.logDebugInfo("[PreprocessingQuery]" + query);
            LogicalPlan parsePlan = new SparkSqlParser(new SQLConf()).parsePlan(query.getQueryStr());
            LogicalPlan filterPlan = getFilterPlan(parsePlan);
            Expression whereConditions = ((Filter) filterPlan).condition();
            String tableName = ((UnresolvedRelation) getUnresolvedRelation(filterPlan)).tableName();
            MySQLTable cacheMySQLTable = PropertiesManager.getProperties().cacheMySQLTableSchemaTypeMap.get(MySQLTable.SchemaType.valueOf(tableName));
            extractTSAndSensorIdConditions(query, cacheMySQLTable, whereConditions, null);
            ArrayList<Sensor> sensors = extractSensors(query);
            for (Sensor sensor : sensors) {
                ArrayList<TimeRange> timeRanges = extractTimeRanges(query);
                query.sensorTimeRangeMap.put(sensor, timeRanges);
            }

        }
        return queries;
    }

    private static LogicalPlan getUnresolvedRelation(LogicalPlan logicalPlan) {
        if (logicalPlan.nodeName().equals("UnresolvedRelation")) return logicalPlan;
        else return getUnresolvedRelation(logicalPlan.children().head());
    }

    private static ArrayList<Sensor> extractSensors(Query query) {
        ArrayList<Sensor> sensors = new ArrayList<>();
        ArrayList<Expression> sensorIdConditions = query.getSensorIdConditions();
        for (Expression sensorIdCondition : sensorIdConditions) {
            if (sensorIdCondition.children().head() instanceof Literal) {
                sensors.add(PropertiesManager.getProperties().sensorMap.get(((UTF8String) ((Literal) sensorIdCondition.children().head()).value()).toString()));
            } else if (sensorIdCondition.children().last() instanceof Literal) {
                sensors.add(PropertiesManager.getProperties().sensorMap.get(((UTF8String) ((Literal) sensorIdCondition.children().last()).value()).toString()));
            }
        }
        return sensors;
    }

    private static ArrayList<TimeRange> extractTimeRanges(Query query) {
        //TODO Check logic and extend for more generic time ranges
        //Very bad code down there. Correct it when you are free.
        ArrayList<TimeRange> timeRanges = new ArrayList<>();
        ArrayList<Expression> tsConditions = query.getTsConditions();
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
                timeRanges.add(new TimeRange(startTimes.get(0), endTimes.get(0)));
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

    private static Long extractTS(Expression tsExpr) {
        if (tsExpr.children().head() instanceof Literal) {
            return ((Integer) ((Literal) tsExpr.children().head()).value()).longValue();
        } else if (tsExpr.children().last() instanceof Literal) {
            return ((Integer) ((Literal) tsExpr.children().last()).value()).longValue();
        }
        return -1L;
    }

    private static LogicalPlan getFilterPlan(LogicalPlan logicalPlan) {
        if (logicalPlan.nodeName().equals("Filter")) return logicalPlan;
        else return getFilterPlan(logicalPlan.children().head());
    }

    private static void extractTSAndSensorIdConditions(Query query, MySQLTable cacheMySQLTable, Expression curExpr, Expression parentExpr) {
        if (curExpr.nodeName().equalsIgnoreCase("UnresolvedAttribute")) {
            if (curExpr.flatArguments().hasNext()) {
                if (((String) curExpr.flatArguments().next()).equalsIgnoreCase(cacheMySQLTable.getTsColumnName())) {
                    query.getTsConditions().add(parentExpr);
                } else if (((String) curExpr.flatArguments().next()).equalsIgnoreCase(cacheMySQLTable.getSensorIdColumnName())) {
                    query.getSensorIdConditions().add(parentExpr);
                }
            }
        }
        Iterator<Expression> iterator = curExpr.children().iterator();
        while (iterator.hasNext()) {
            extractTSAndSensorIdConditions(query, cacheMySQLTable, iterator.next(), curExpr);
        }
    }

    @Override
    public String toString() {
        return "Query{" +
                "queryTimestamp=" + queryTimestamp +
                ", queryStr='" + queryStr + '\'' +
                ", userHost='" + userHost + '\'' +
                ", tsConditions=" + tsConditions +
                ", sensorIdConditions=" + sensorIdConditions +
                ", sensorTimeRangeMap=" + sensorTimeRangeMap +
                '}';
    }

    public Timestamp getQueryTimestamp() {
        return queryTimestamp;
    }

    public void setQueryTimestamp(Timestamp queryTimestamp) {
        this.queryTimestamp = queryTimestamp;
    }

    public String getQueryStr() {
        return queryStr;
    }

    public void setQueryStr(String queryStr) {
        this.queryStr = queryStr;
    }

    public ArrayList<Expression> getTsConditions() {
        return tsConditions;
    }

    public ArrayList<Expression> getSensorIdConditions() {
        return sensorIdConditions;
    }


    public Map<Sensor, List<TimeRange>> getSensorTimeRangeMap() {
        return sensorTimeRangeMap;
    }

    public String getUserHost() {
        return userHost;
    }

    public void setUserHost(String userHost) {
        this.userHost = userHost;
    }

    static class TimeRange {
        Long startTime;
        Long endTime;

        public TimeRange(Long startTime, Long endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }

        @Override
        public String toString() {
            return "TimeRange{" +
                    "startDate=" + new Date(startTime * 1000) +
                    ", endDate=" + new Date(endTime * 1000) +
                    '}';
        }
    }

}
