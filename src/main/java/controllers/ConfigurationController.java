package controllers;

import beans.*;
import com.google.gson.*;
import managers.Utils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileReader;
import static beans.ConfigurationBean.SchemaType;
import static managers.Utils.df;
import static managers.Utils.getTimeInSec;

public class ConfigurationController {

    public ConfigurationBean readConfiguration(String configurationFilePath) {
        ConfigurationBean cb = new ConfigurationBean();
        Utils.configurationBean = cb;
        GsonBuilder builder = new GsonBuilder();
        builder.setPrettyPrinting();
        Gson gson = builder.create();

        try {
            JsonObject jsonObject = gson.fromJson(new FileReader(configurationFilePath), JsonObject.class);
            cb.setJsonObject(jsonObject);

            //Other Properties
            cb.bitmapStartTime = df.parse(jsonObject.get("bitmapStartTime").getAsString());
            cb.bitmapEndTime = df.parse(jsonObject.get("bitmapEndTime").getAsString());
            cb.queryLogCleanupDurationInSeconds = jsonObject.get("queryLogCleanupDurationInSeconds").getAsInt();
            cb.queryPollingDurationInSeconds = jsonObject.get("queryPollingDurationInSeconds").getAsInt();
            cb.reportReceiverEmail = jsonObject.get("reportReceiverEmail").getAsString();
            cb.logFilePath = jsonObject.get("logFilePath").getAsString();
            cb.appName = jsonObject.get("appName").getAsString();
            cb.reportError = jsonObject.get("reportError").getAsBoolean();
            cb.debug = jsonObject.get("debug").getAsBoolean();
            cb.stopSparkLogging = jsonObject.get("stopSparkLogging").getAsBoolean();


            //Granularity properties
            JsonArray granularities = jsonObject.getAsJsonArray("granularities");
            for (JsonElement granularityElement : granularities) {
                JsonObject granularity = granularityElement.getAsJsonObject();
                GranularityBean granularityBean = new GranularityBean();
                granularityBean.setGranularityId(granularity.get("granularityId").getAsString());
                granularityBean.setWindowDuration(granularity.get("windowDuration").getAsString());
                granularityBean.setDisplayLimitInSeconds(granularity.get("displayLimitInSeconds").getAsInt());
                granularityBean.setGranularityInTermsOfSeconds(granularity.get("inTermsOfSeconds").getAsInt());
                granularityBean.setDisplayPriority(granularity.get("displayPriority").getAsInt());
                granularityBean.setFetchIntervalAtOnceInSeconds(granularity.get("fetchIntervalAtOnceInSeconds").getAsInt());
                granularityBean.setNumPartitionsForEachInterval(granularity.get("numPartitionsForEachInterval").getAsInt());
                granularityBean.setNumParallelQuery(granularity.get("numParallelQuery").getAsInt());
                cb.granularityBeanMap.put(granularityBean.getGranularityId(), granularityBean);
            }

            JsonArray databases = jsonObject.getAsJsonArray("databases");
            for (JsonElement databaseElement : databases) {
                JsonObject database = databaseElement.getAsJsonObject();
                DatabaseBean databaseBean = new DatabaseBean();
                databaseBean.setDatabaseId(database.get("databaseId").getAsString());
                databaseBean.setDatabaseType(database.get("databaseType").getAsString());
                databaseBean.setDatabaseName(database.get("databaseName").getAsString());
                databaseBean.setHost(database.get("host").getAsString());
                databaseBean.setPort(database.get("port").getAsString());
                databaseBean.setUsername(database.get("username").getAsString());
                databaseBean.setPassword(database.get("password").getAsString());
                cb.databaseBeanMap.put(databaseBean.getDatabaseId(), databaseBean);
            }

            JsonArray flCacheTables = jsonObject.getAsJsonArray("flCacheTables");
            for (JsonElement flCacheTableElement : flCacheTables) {
                JsonObject flCacheTable = flCacheTableElement.getAsJsonObject();
                FLCacheTableBean flCacheTableBean = new FLCacheTableBean();
                flCacheTableBean.setTableId(flCacheTable.get("tableId").getAsString());
                flCacheTableBean.setDatabaseBean(cb.databaseBeanMap.get(flCacheTable.get("databaseId").getAsString()));
                flCacheTableBean.setSchemaType(SchemaType.valueOf(flCacheTable.get("schemaType").getAsString()));
                flCacheTableBean.setSensorIdColumnName(flCacheTable.get("sensorIdColumnName").getAsString());
                flCacheTableBean.setTableName(flCacheTable.get("tableName").getAsString());
                flCacheTableBean.setTsColumnName(flCacheTable.get("tsColumnName").getAsString());
                cb.flCacheTableBeanMap.put(flCacheTableBean.getTableId(), flCacheTableBean);
            }

            JsonArray slCacheTables = jsonObject.getAsJsonArray("slCacheTables");
            for (JsonElement slCacheTableElement : slCacheTables) {
                JsonObject slCacheTable = slCacheTableElement.getAsJsonObject();
                SLCacheTableBean slCacheTableBean = new SLCacheTableBean();
                slCacheTableBean.setTableId(slCacheTable.get("tableId").getAsString());
                slCacheTableBean.setDatabaseBean(cb.databaseBeanMap.get(slCacheTable.get("databaseId").getAsString()));
                slCacheTableBean.setSchemaType(SchemaType.valueOf(slCacheTable.get("schemaType").getAsString()));
                slCacheTableBean.setSensorIdColumnName(slCacheTable.get("sensorIdColumnName").getAsString());
                slCacheTableBean.setTableName(slCacheTable.get("tableName").getAsString());
                slCacheTableBean.setTsColumnName(slCacheTable.get("tsColumnName").getAsString());
                cb.slCacheTableBeanMap.put(slCacheTableBean.getTableId(), slCacheTableBean);
            }

            JsonArray sourceTables = jsonObject.getAsJsonArray("sourceTables");
            for (JsonElement sourceTableElement : sourceTables) {
                JsonObject sourceTable = sourceTableElement.getAsJsonObject();
                SourceTableBean sourceTableBean = new SourceTableBean();
                sourceTableBean.setTableId(sourceTable.get("tableId").getAsString());
                sourceTableBean.setDatabaseBean(cb.databaseBeanMap.get(sourceTable.get("databaseId").getAsString()));
                sourceTableBean.setSchemaType(SchemaType.valueOf(sourceTable.get("schemaType").getAsString()));
                sourceTableBean.setSensorIdColumnName(sourceTable.get("sensorIdColumnName").getAsString());
                sourceTableBean.setTableName(sourceTable.get("tableName").getAsString());
                sourceTableBean.setTsColumnName(sourceTable.get("tsColumnName").getAsString());
                cb.sourceTableBeanMap.put(sourceTableBean.getTableId(), sourceTableBean);
            }

            JsonArray sensors = jsonObject.getAsJsonArray("sensors");
            for (JsonElement sensorElement : sensors) {
                JsonObject sensor = sensorElement.getAsJsonObject();
                SensorBean sensorBean = new SensorBean();
                sensorBean.setSensorId(sensor.get("sensorId").getAsString());
                sensorBean.setFlCacheTableBean(cb.flCacheTableBeanMap.get(sensor.get("flCacheTableId").getAsString()));

                JsonArray slCacheTableIds = sensor.getAsJsonArray("slCacheTableIds");
                for (JsonElement jsonElement : slCacheTableIds) {
                    JsonObject timeVsTable = jsonElement.getAsJsonObject();
                    Pair<TimeRangeBean, SLCacheTableBean> pair = new MutablePair<>(
                            new TimeRangeBean(getTimeInSec(df.parse(timeVsTable.get("startTime").getAsString()))
                                    ,getTimeInSec(df.parse(timeVsTable.get("endTime").getAsString()))),
                            cb.slCacheTableBeanMap.get(timeVsTable.get("tableId").getAsString())
                    );
                    sensorBean.getTimeRangeVsSLCacheTables().add(pair);
                }
                JsonArray sourceTableIds = sensor.getAsJsonArray("sourceTableIds");
                for (JsonElement jsonElement : sourceTableIds) {
                    JsonObject timeVsTable = jsonElement.getAsJsonObject();
                    Pair<TimeRangeBean, SourceTableBean> pair = new MutablePair<>(
                            new TimeRangeBean(getTimeInSec(df.parse(timeVsTable.get("startTime").getAsString()))
                                    ,getTimeInSec(df.parse(timeVsTable.get("endTime").getAsString()))),
                            cb.sourceTableBeanMap.get(timeVsTable.get("tableId").getAsString())
                    );
                    sensorBean.getTimeRangeVsSourceTables().add(pair);
                }
                sensorBean.setFlBitmapBean(new BitmapBean(cb.bitmapStartTime, cb.bitmapEndTime, cb.granularityBeanMap));
                sensorBean.setSlBitmapBean(new BitmapBean(cb.bitmapStartTime, cb.bitmapEndTime, cb.granularityBeanMap));
                cb.sensorBeanMap.put(sensorBean.getSensorId(), sensorBean);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


        return cb;
    }


}
