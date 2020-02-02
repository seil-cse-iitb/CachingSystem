package beans;

import com.google.gson.JsonObject;

import java.util.Date;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

public class ConfigurationBean {
    public final Map<String, GranularityBean> granularityBeanMap = new HashMap<>();
    public final Map<String, DatabaseBean> databaseBeanMap = new HashMap<>();
    public final Map<String, FLCacheTableBean> flCacheTableBeanMap = new HashMap<>();
    public final Map<String, SLCacheTableBean> slCacheTableBeanMap = new HashMap<>();
    public final Map<String, SourceTableBean> sourceTableBeanMap = new HashMap<>();
    public final Map<String, SensorBean> sensorBeanMap = new HashMap<>();
//    public final Map<String, SensorGroupBean> sensorGroupBeanMap = new HashMap<>();

    public int queryLogCleanupDurationInSeconds = 300;
    public int queryPollingDurationInSeconds = 5;
    public boolean debug = false;
    public String reportReceiverEmail = "sapantanted99@gmail.com";
    public String logFilePath = "/tmp/cache-system.log";
    public String appName = "CacheSystem";
    public boolean reportError = true;
    public boolean stopSparkLogging = false;
    public String granularityTableNameSuffix = "granularity";
    public String bitmapTableNameSuffix = "bitmap";
    public Date bitmapStartTime;
    public Date bitmapEndTime;
    public String schedulerAllocationFile = null;
    JsonObject jsonObject;

    public int getQueryLogCleanupDurationInSeconds() {
        return queryLogCleanupDurationInSeconds;
    }

    public void setQueryLogCleanupDurationInSeconds(int queryLogCleanupDurationInSeconds) {
        this.queryLogCleanupDurationInSeconds = queryLogCleanupDurationInSeconds;
    }

    public int getQueryPollingDurationInSeconds() {
        return queryPollingDurationInSeconds;
    }

    public void setQueryPollingDurationInSeconds(int queryPollingDurationInSeconds) {
        this.queryPollingDurationInSeconds = queryPollingDurationInSeconds;
    }

    public Date getBitmapStartTime() {
        return bitmapStartTime;
    }

    public void setBitmapStartTime(Date bitmapStartTime) {
        this.bitmapStartTime = bitmapStartTime;
    }

    public Date getBitmapEndTime() {
        return bitmapEndTime;
    }

    public void setBitmapEndTime(Date bitmapEndTime) {
        this.bitmapEndTime = bitmapEndTime;
    }

    public JsonObject getJsonObject() {
        return jsonObject;
    }

    public void setJsonObject(JsonObject jsonObject) {
        this.jsonObject = jsonObject;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public String getReportReceiverEmail() {
        return reportReceiverEmail;
    }

    public void setReportReceiverEmail(String reportReceiverEmail) {
        this.reportReceiverEmail = reportReceiverEmail;
    }

    public String getLogFilePath() {
        return logFilePath;
    }

    public void setLogFilePath(String logFilePath) {
        this.logFilePath = logFilePath;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public boolean isReportError() {
        return reportError;
    }

    public void setReportError(boolean reportError) {
        this.reportError = reportError;
    }

    public boolean isStopSparkLogging() {
        return stopSparkLogging;
    }

    public void setStopSparkLogging(boolean stopSparkLogging) {
        this.stopSparkLogging = stopSparkLogging;
    }

    public String getGranularityTableNameSuffix() {
        return granularityTableNameSuffix;
    }

    public void setGranularityTableNameSuffix(String granularityTableNameSuffix) {
        this.granularityTableNameSuffix = granularityTableNameSuffix;
    }

    public String getBitmapTableNameSuffix() {
        return bitmapTableNameSuffix;
    }

    public void setBitmapTableNameSuffix(String bitmapTableNameSuffix) {
        this.bitmapTableNameSuffix = bitmapTableNameSuffix;
    }

    public enum SchemaType {power, temperature, temperature_humidity}


}
