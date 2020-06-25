package controllers;
import beans.GranularityBean;
import beans.LiveStreamBean;
import managers.LogManager;
import managers.Parser;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.internal.config.OptionalConfigEntry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;

import static org.apache.spark.sql.functions.col;

public class LiveStreamController {
    public static final boolean START = true;
    public static final boolean END = false;
    private CacheSystemController c;

    public LiveStreamController(CacheSystemController c) {
        this.c = c;
    }

    public void startStream(LiveStreamBean liveStreamBean, GranularityBean granularityBean) {
        c.sparkSession.sparkContext().conf().set("spark.default.parallelism","1");
        c.sparkSession.sessionState().conf().setConf(SQLConf.SHUFFLE_PARTITIONS(), 1);
        //TODO check the effect of above configs on other functionalities. check how can we change this settings only for the streaming query
        Parser parser = new Parser(liveStreamBean.getSensorBean().flCacheTableBean.getSchemaType());
        LogManager.logInfo("Live Stream Started: "+liveStreamBean);
        String clientId= liveStreamBean.getSensorBean().getSensorId()+"_"+granularityBean.getGranularityId()+"_live_stream";
        Dataset<Row> streamingDataset = c.sparkSession.readStream()
                .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
                .option("topic", liveStreamBean.getTopic())
                .option("localStorage", "/tmp/spark-mqtt/")
                .option("QoS", c.cb.mqttQos)
                .option("clientId", clientId)
                .load(c.cb.mqttUrl);
        streamingDataset = streamingDataset.withColumn("payload",col("payload").cast(DataTypes.StringType));
        String sensorId = liveStreamBean.getSensorBean().getSensorId();
        streamingDataset = streamingDataset
                .map((MapFunction<Row, Row>) row -> {
                    Integer id = row.getInt(row.fieldIndex("id"));
                    String payload = row.getString(row.fieldIndex("payload"));
                    String topic = row.getString(row.fieldIndex("topic"));
                    Timestamp timestamp = row.getTimestamp(row.fieldIndex("timestamp"));
                    return parser.parse(id,topic,timestamp,sensorId,payload);
                }, parser.getEncoder());

        // Initiate the start time, accumulator and other parameter of the live stream
        liveStreamBean.initStreamParameters();
        LogManager.logDebugInfo("live stream aggregation for granularity: "+granularityBean.getGranularityId());
        Dataset<Row> aggregatedStreamingDataset = c.aggregationManager.aggregateFromSource(streamingDataset, granularityBean, parser.getSourceTableSchema());
//        aggregatedStreamingDataset = aggregatedStreamingDataset
//                .withColumn(parser.getSourceTableSchema().getTsColumnName(),col(parser.getSourceTableSchema().getTsColumnName()).cast(DataTypes.TimestampType))
//                .withWatermark(parser.getSourceTableSchema().getTsColumnName(), "1 minute")
//        ;
        aggregatedStreamingDataset = aggregatedStreamingDataset
                    .select("sensor_id","ts","granularityId","count_agg_rows","sum_power_1");
        aggregatedStreamingDataset.printSchema();

        Dataset<Row> finalAggregatedStreamingDataset = aggregatedStreamingDataset;
        StreamingQuery console = finalAggregatedStreamingDataset
                .withColumn("ts",col("ts").cast(DataTypes.TimestampType))
                .withWatermark("ts",granularityBean.getWindowDuration())
                .writeStream()
                .trigger(Trigger.ProcessingTime(granularityBean.getWindowDuration()))
                .outputMode(OutputMode.Update())
                .foreach(parser.getForeachWriter())
                .start();
        try {
            console.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
            LogManager.logError("Live Stream of sensor: "+sensorId+" "+e.getMessage());
        }
        //TODO accumulators will be updated when storing in database. One thread will periodically check for accumulator's value and update the bitmap accordingly.

    }

    public void stopStream(LiveStreamBean liveStreamBean) {
        //TODO check accumulator before removing liveSstreamBean and update bitmap accordingly
//        liveStreamBean.getLiveStreamThread().interrupt();
        LogManager.logInfo("Live Stream Stopped: "+liveStreamBean);
    }
}
