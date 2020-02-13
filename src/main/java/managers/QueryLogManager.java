package managers;

import beans.DatabaseBean;
import beans.FLCacheTableBean;
import beans.QueryBean;
import controllers.CacheSystemController;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;

public class QueryLogManager {
    CacheSystemController c;
    private Timestamp cachedResultTill;
    private Set<DatabaseBean> databaseBeans;

    public QueryLogManager(CacheSystemController c) {
        this.c = c;
        this.cachedResultTill = new Timestamp(System.currentTimeMillis());
        databaseBeans = new HashSet<>();
        for (FLCacheTableBean flCacheTableBean : c.cb.flCacheTableBeanMap.values()) {
            databaseBeans.add(flCacheTableBean.getDatabaseBean());
        }
    }

    public void startQueryLogCleanupThread() {
        LogManager.logInfo("[Starting cache databases query logging]");
        for (DatabaseBean db : databaseBeans) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    c.sparkSession.sparkContext().setLocalProperty("spark.scheduler.pool", "queryLogCleanupThread");
                    while (true) {
                        LogManager.logInfo("[Query log cleanup][" + db.getHost() + "]");
                        Connection connection = null;
                        try {
                            connection = DriverManager.getConnection(c.databaseController.getURL(db, "mysql"), c.databaseController.getProperties(db));
                            assert connection.prepareStatement("START TRANSACTION").executeUpdate() == 0;
                            assert connection.prepareStatement("SET GLOBAL general_log = 'OFF'").executeUpdate() == 0;
                            assert connection.prepareStatement("RENAME TABLE general_log TO general_log_temp").executeUpdate() == 0;
                            assert connection.prepareStatement("DELETE from mysql.general_log_temp where event_time<'" + cachedResultTill + "'").executeUpdate() >= 0;
                            assert connection.prepareStatement("RENAME TABLE general_log_temp TO general_log").executeUpdate() == 0;
                            assert connection.prepareStatement("SET GLOBAL general_log = 'ON'").executeUpdate() == 0;
                            assert connection.prepareStatement("SET GLOBAL log_output = 'TABLE'").executeUpdate() == 0;
                            assert connection.prepareStatement("SET GLOBAL max_allowed_packet = 1073741824").executeUpdate() == 0;
                            assert connection.prepareStatement("COMMIT").executeUpdate() == 0;
                            connection.close();
                        } catch (SQLException e) {
                            LogManager.logError("[" + this.getClass() + "][startQueryLoggingThread][" + db.getDatabaseId() + "]" + e.getMessage());
                        } finally {
                            if (connection != null) {
                                try {
                                    connection.close();
                                } catch (SQLException e) {
                                    LogManager.logError("[" + this.getClass() + "][connection closing exception]" + e.getMessage());
                                }
                            }
                        }
                        try {
                            Thread.sleep(c.cb.queryLogCleanupDurationInSeconds * 1000);
                        } catch (InterruptedException e) {
                            LogManager.logError("[" + this.getClass() + "][startQueryLoggingThread][" + db.getDatabaseId() + "]" + e.getMessage());
                        }
                    }
                }
            }).start();
        }
    }

    public List<QueryBean> getNewQueries() {
        Timestamp lastFetchTime = new Timestamp(this.cachedResultTill.getTime());
        try {
            DatabaseBean queryDumpDB = c.cb.databaseBeanMap.get("query_dump");
            List<QueryBean> totalQueries = new ArrayList<>();
            this.cachedResultTill.setTime(System.currentTimeMillis());
            for (DatabaseBean db : databaseBeans) {
                c.sparkSession.sparkContext().setLocalProperty("callSite.short", "Fetching new queries after "+lastFetchTime);
                c.sparkSession.sparkContext().setLocalProperty("callSite.long", "Fetching new queries after "+lastFetchTime+" till "+this.cachedResultTill);
                Dataset<Row> generalLog = c.sparkSession.read().jdbc(c.databaseController.getURL(db, "mysql"), "general_log", c.databaseController.getProperties(db));
                Dataset<QueryBean> queryBeanDataset = generalLog
                        .where("event_time < '" + this.cachedResultTill + "' and event_time >= '" + lastFetchTime + "' and command_type='Query' and argument like '%meta_data%' and argument not like '%ignore_cache%' and argument not like '%general_log%'")
                        .select(col("user_host"), col("event_time"), regexp_replace(regexp_replace(col("argument").cast("String"), "\n", " "), "  *", " ").as("argument"))
                        .map(QueryBean.queryMapFunction, Encoders.bean(QueryBean.class))
                        .persist();
                List<QueryBean> queries = queryBeanDataset.collectAsList();
                if(c.cb.storeVisualizationQueries) {
                    c.sparkSession.sparkContext().setLocalProperty("callSite.short", "Storing new queries after " + lastFetchTime);
                    c.sparkSession.sparkContext().setLocalProperty("callSite.long", "Storing new queries after " + lastFetchTime + " till " + this.cachedResultTill + " into " + queryDumpDB);
                    queryBeanDataset.write().mode(SaveMode.Append).jdbc(c.databaseController.getURL(queryDumpDB, queryDumpDB.getDatabaseName()), "query_log", c.databaseController.getProperties(queryDumpDB));
                }
                queryBeanDataset.unpersist();
                totalQueries.addAll(c.queryController.preprocessQueries(queries));
            }
            c.sparkSession.sparkContext().setLocalProperty("callSite.short",null);
            c.sparkSession.sparkContext().setLocalProperty("callSite.long", null);
            LogManager.logInfo("[New queries fetched][cachedResultTill:" + new Date(cachedResultTill.getTime()) + "][queries_count:" + totalQueries.size() + "]");
            return totalQueries;
        } catch (Exception e) {
            this.cachedResultTill.setTime(lastFetchTime.getTime());
            LogManager.logError("[" + this.getClass() + "][Fetching queries]" + e.getMessage());
        }
        return new ArrayList<>();
    }
}
