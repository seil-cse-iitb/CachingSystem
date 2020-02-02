package beans;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryBean implements Serializable {
    public static MapFunction<Row, QueryBean> queryMapFunction = new MapFunction<Row, QueryBean>() {
        public QueryBean call(Row value) throws Exception {
            Timestamp queryTimestamp = value.getTimestamp(value.fieldIndex("event_time"));
            String queryStr = value.getString(value.fieldIndex("argument"));
            String userHost = value.getAs("user_host");
            QueryBean query = new QueryBean();
            query.setQueryStr(queryStr);
            query.setQueryTimestamp(queryTimestamp);
            query.setUserHost(userHost);
            return query;
        }
    };
    private final Map<SensorBean, List<TimeRangeBean>> sensorTimeRangeListMap = new HashMap<>();
    private Timestamp queryTimestamp;
    private String queryStr;
    private String userHost;
    private final Map<String,String> metaData=new HashMap<>();

    public Map<SensorBean, List<TimeRangeBean>> getSensorTimeRangeListMap() {
        return sensorTimeRangeListMap;
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

    public String getUserHost() {
        return userHost;
    }

    public void setUserHost(String userHost) {
        this.userHost = userHost;
    }

    public Map<String, String> getMetaData() {
        return metaData;
    }

    @Override
    public String toString() {
        return "QueryBean{" +
                "queryTimestamp=" + queryTimestamp +
                ", queryStr='" + queryStr + '\'' +
                ", userHost='" + userHost + '\'' +
                '}';
    }

}
