package version1.managerOld;

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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.util.Collections.sort;

public class QueryOld implements Serializable {


    public static MapFunction<Row, QueryOld> queryMapFunction = new MapFunction<Row, QueryOld>() {
        public QueryOld call(Row value) throws Exception {
            Timestamp queryTimestamp = value.getTimestamp(value.fieldIndex("event_time"));
            String queryStr = value.getString(value.fieldIndex("argument"));
            String userHost = value.getAs("user_host");
            QueryOld queryOld = new QueryOld(queryTimestamp, queryStr, userHost);
            return queryOld;
        }
    };

    public String granularity;
    public String tableName;
    public String tsColumnName="TS";
    public String sensorIdColumnName="sensor_id";
    private Timestamp queryTimestamp;
    private String queryStr;
    private String userHost;
    private ArrayList<Expression> tsConditions = new ArrayList<>();
    private ArrayList<Expression> sensorIdConditions = new ArrayList<>();
    private ArrayList<TimeRange> timeRanges = new ArrayList<>();
    private ArrayList<String> sensorIds = new ArrayList<>();

    public QueryOld() {
    }

    public QueryOld(Timestamp queryTimestamp, String queryStr, String userHost) {
        this.queryTimestamp = queryTimestamp;
        this.queryStr = queryStr;
        this.userHost = userHost;
    }

    public static List<QueryOld> preprocessQueries(List<QueryOld> queries) {
        for (QueryOld queryOld : queries) {
            LogicalPlan parsePlan = new SparkSqlParser(new SQLConf()).parsePlan(queryOld.getQueryStr());
            LogicalPlan filterPlan = getFilterPlan(parsePlan);
            Expression whereConditions = ((Filter)filterPlan).condition();
            getTSAndSensorIdConditions(queryOld,whereConditions, null, queryOld.getTsConditions(), queryOld.getSensorIdConditions());
            queryOld.tableName = ((UnresolvedRelation) getUnresolvedRelation(filterPlan)).tableName();
            extractSensorIds(queryOld);
            calculateTimeRanges(queryOld);
            calculateGranularity(queryOld);
        }
        return queries;
    }

    private static LogicalPlan getUnresolvedRelation(LogicalPlan logicalPlan) {
        if (logicalPlan.nodeName().equals("UnresolvedRelation")) return logicalPlan;
        else return getUnresolvedRelation(logicalPlan.children().head());
    }

    private static void extractSensorIds(QueryOld queryOld) {
        ArrayList<Expression> sensorIdConditions = queryOld.getSensorIdConditions();
        for (Expression sensorIdCondition : sensorIdConditions) {
            if (sensorIdCondition.children().head() instanceof Literal) {
                queryOld.getSensorIds().add(((UTF8String) ((Literal) sensorIdCondition.children().head()).value()).toString());
            } else if (sensorIdCondition.children().last() instanceof Literal) {
                queryOld.getSensorIds().add(((UTF8String) ((Literal) sensorIdCondition.children().last()).value()).toString());
            }
        }
    }

    private static void calculateGranularity(QueryOld queryOld) {
        ArrayList<TimeRange> timeRanges = queryOld.getTimeRanges();
        long totalTime = 0;
        for (TimeRange timeRange : timeRanges) {
            totalTime += timeRange.endTime - timeRange.startTime;
        }
        if (totalTime <= 4 * 24 * 60 * 60) { // less than 4 Days
            queryOld.granularity = "1 minute";
        } else if (totalTime <= 2 * 30 * 24 * 60 * 60) { // less than 2 months
            queryOld.granularity = "1 hour";
        } else if (totalTime <= 370 * 24 * 60 * 60) { // less than 1 year
            queryOld.granularity = "1 day";
        } else {
            queryOld.granularity = "1 day";
        }
    }

    private static void calculateTimeRanges(QueryOld queryOld) {
        //TODO Check logic and extend for more generic time ranges
        //Very bad code down there. Correct it when you are free.
        ArrayList<Expression> tsConditions = queryOld.getTsConditions();
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
                queryOld.getTimeRanges().add(new TimeRange(startTimes.get(0), endTimes.get(0)));
                startTimes.remove(0);
                endTimes.remove(0);
            } else {
                //TODO edit in previous TimeRange
            }

        }
//        System.out.println("Remaining StartTimes: " + startTimes);
//        System.out.println("Remaining EndTimes: " + endTimes);
//        System.out.println("Proper TimeRanges: " + timeRanges);
    }

    private static Long extractTS(Expression tsExpr) {
        if (tsExpr.children().head() instanceof Literal) {
            return ((Integer) ((Literal) tsExpr.children().head()).value()).longValue();
        } else if (tsExpr.children().last() instanceof Literal) {
            return ((Integer) ((Literal) tsExpr.children().last()).value()).longValue();
        }
        return -1l;
    }

    private static LogicalPlan getFilterPlan(LogicalPlan logicalPlan) {
        if (logicalPlan.nodeName().equals("Filter")) return logicalPlan;
        else return getFilterPlan(logicalPlan.children().head());
    }

    private static void getTSAndSensorIdConditions(QueryOld queryOld, Expression curExpr, Expression parentExpr, ArrayList<Expression> tsConditions, ArrayList<Expression> sensorIdConditions) {
        if (curExpr.nodeName().equalsIgnoreCase("UnresolvedAttribute")) {
            if (curExpr.flatArguments().hasNext()) {
                if (((String) curExpr.flatArguments().next()).equals(queryOld.tsColumnName)) {
                    tsConditions.add(parentExpr);
                } else if (curExpr.flatArguments().next().equals(queryOld.sensorIdColumnName)) {
                    sensorIdConditions.add(parentExpr);
                }
            }
        }
        Iterator<Expression> iterator = curExpr.children().iterator();
        while (iterator.hasNext()) {
            getTSAndSensorIdConditions(queryOld, iterator.next(), curExpr, tsConditions, sensorIdConditions);
        }
    }

    @Override
    public String toString() {
        return "Query{" +
                "granularity='" + granularity + '\'' +
                ", tableName='" + tableName + '\'' +
                ", tsColumnName='" + tsColumnName + '\'' +
                ", sensorIdColumnName='" + sensorIdColumnName + '\'' +
                ", queryTimestamp=" + queryTimestamp +
                ", queryStr='" + queryStr + '\'' +
                ", userHost='" + userHost + '\'' +
                ", tsConditions=" + tsConditions +
                ", sensorIdConditions=" + sensorIdConditions +
                ", timeRanges=" + timeRanges +
                ", sensorIds=" + sensorIds +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryOld)) return false;

        QueryOld queryOld = (QueryOld) o;

        if (!getQueryTimestamp().equals(queryOld.getQueryTimestamp())) return false;
        if (!getQueryStr().equals(queryOld.getQueryStr())) return false;
        if (!getTsConditions().equals(queryOld.getTsConditions())) return false;
        return getSensorIdConditions().equals(queryOld.getSensorIdConditions());
    }

    @Override
    public int hashCode() {
        int result = getQueryTimestamp().hashCode();
        result = 31 * result + getQueryStr().hashCode();
        result = 31 * result + getTsConditions().hashCode();
        result = 31 * result + getSensorIdConditions().hashCode();
        return result;
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

    public ArrayList<TimeRange> getTimeRanges() {
        return timeRanges;
    }

    public ArrayList<String> getSensorIds() {
        return sensorIds;
    }

    public void setSensorIds(ArrayList<String> sensorIds) {
        this.sensorIds = sensorIds;
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
