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
    static GsonBuilder builder;
    static Gson gson;
    public ConfigurationController(){
        builder = new GsonBuilder();
        builder.setPrettyPrinting();
        gson = builder.create();
    }
    public ConfigurationBean readConfiguration(String configurationFilePath) {
        ConfigurationBean cb = new ConfigurationBean();
        Utils.configurationBean = cb;

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
            cb.schedulerAllocationFile = jsonObject.get("schedulerAllocationFile").getAsString();
            cb.appName = jsonObject.get("appName").getAsString();
            cb.reportError = jsonObject.get("reportError").getAsBoolean();
            cb.debug = jsonObject.get("debug").getAsBoolean();
            cb.stopSparkLogging = jsonObject.get("stopSparkLogging").getAsBoolean();
            cb.storeVisualizationQueries = jsonObject.get("storeVisualizationQueries").getAsBoolean();
            cb.mqttUrl = jsonObject.get("mqttUrl").getAsString();
            cb.mqttQos = jsonObject.get("mqttQos").getAsInt();

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
                SchemaType schemaType = sensorBean.getFlCacheTableBean().getSchemaType();
                JsonArray slCacheTableIds = sensor.getAsJsonArray("slCacheTableIds");
                for (JsonElement jsonElement : slCacheTableIds) {
                    JsonObject timeVsTable = jsonElement.getAsJsonObject();
                    SLCacheTableBean slCacheTableBean = cb.slCacheTableBeanMap.get(timeVsTable.get("tableId").getAsString());
                    assert schemaType == slCacheTableBean.getSchemaType();
                    Pair<TimeRangeBean, SLCacheTableBean> pair = new MutablePair<>(
                            new TimeRangeBean(getTimeInSec(df.parse(timeVsTable.get("startTime").getAsString()))
                                    , getTimeInSec(df.parse(timeVsTable.get("endTime").getAsString()))),
                            slCacheTableBean);
                    sensorBean.getTimeRangeVsSLCacheTables().add(pair);
                }
                JsonArray sourceTableIds = sensor.getAsJsonArray("sourceTableIds");
                for (JsonElement jsonElement : sourceTableIds) {
                    JsonObject timeVsTable = jsonElement.getAsJsonObject();
                    SourceTableBean sourceTableBean = cb.sourceTableBeanMap.get(timeVsTable.get("tableId").getAsString());
                    assert schemaType==sourceTableBean.getSchemaType();
                    Pair<TimeRangeBean, SourceTableBean> pair = new MutablePair<>(
                            new TimeRangeBean(getTimeInSec(df.parse(timeVsTable.get("startTime").getAsString()))
                                    , getTimeInSec(df.parse(timeVsTable.get("endTime").getAsString()))),
                            sourceTableBean
                    );
                    sensorBean.getTimeRangeVsSourceTables().add(pair);
                }
                sensorBean.setFlBitmapBean(new BitmapBean(cb.bitmapStartTime, cb.bitmapEndTime, cb.granularityBeanMap));
                sensorBean.setSlBitmapBean(new BitmapBean(cb.bitmapStartTime, cb.bitmapEndTime, cb.granularityBeanMap));
                cb.sensorBeanMap.put(sensorBean.getSensorId(), sensorBean);
            }

            JsonArray sensorGroups = jsonObject.getAsJsonArray("sensorGroups");
            for (JsonElement sensorGroupElement : sensorGroups) {
                JsonObject sensorGroup = sensorGroupElement.getAsJsonObject();
                SensorGroupBean sensorGroupBean = new SensorGroupBean();
                sensorGroupBean.setSensorId(sensorGroup.get("sensorId").getAsString());
                sensorGroupBean.setSpatialAggFunction(sensorGroup.get("spatialAggFunction").getAsString());

                sensorGroupBean.setFlCacheTableBean(cb.flCacheTableBeanMap.get(sensorGroup.get("flCacheTableId").getAsString()));
                SchemaType schemaType = sensorGroupBean.getFlCacheTableBean().getSchemaType();
                JsonArray slCacheTableIds = sensorGroup.getAsJsonArray("slCacheTableIds");
                for (JsonElement jsonElement : slCacheTableIds) {
                    JsonObject timeVsTable = jsonElement.getAsJsonObject();
                    SLCacheTableBean slCacheTableBean = cb.slCacheTableBeanMap.get(timeVsTable.get("tableId").getAsString());
                    assert schemaType == slCacheTableBean.getSchemaType();
                    Pair<TimeRangeBean, SLCacheTableBean> pair = new MutablePair<>(
                            new TimeRangeBean(getTimeInSec(df.parse(timeVsTable.get("startTime").getAsString()))
                                    , getTimeInSec(df.parse(timeVsTable.get("endTime").getAsString()))),
                            slCacheTableBean);
                    sensorGroupBean.getTimeRangeVsSLCacheTables().add(pair);
                }
                sensorGroupBean.setFlBitmapBean(new BitmapBean(cb.bitmapStartTime, cb.bitmapEndTime, cb.granularityBeanMap));
                sensorGroupBean.setSlBitmapBean(new BitmapBean(cb.bitmapStartTime, cb.bitmapEndTime, cb.granularityBeanMap));

                schemaType = null;
                JsonArray sensorGroupSensors = sensorGroup.getAsJsonArray("sensors");
                for (JsonElement sensorElement : sensorGroupSensors) {
                    SensorBean sensorBean = cb.sensorBeanMap.get(sensorElement.getAsString());
                    if (schemaType == null) schemaType = sensorBean.getFlCacheTableBean().getSchemaType();
                    else assert schemaType == sensorBean.getFlCacheTableBean().getSchemaType();
                    sensorGroupBean.getSensorList().add(sensorBean);
                }
                assert !sensorGroupBean.getSensorList().isEmpty();
                cb.sensorBeanMap.put(sensorGroupBean.getSensorId(), sensorGroupBean);
//                cb.sensorGroupBeanMap.put(sensorGroupBean.getSensorId(), sensorGroupBean);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        return cb;
    }


}
