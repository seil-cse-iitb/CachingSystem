package managers;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class LogManager {
    public final static String logFilePath = Utils.configurationBean.logFilePath;

    public static void log(String text) {
        System.out.println("[" + Utils.current_timestamp_str() + "][Thread:" + Thread.currentThread().getName() + "]" + (text));
        File logFile = new File(LogManager.logFilePath);
        try {
            FileWriter fw = new FileWriter(logFile, true);
            fw.append("[" + Utils.current_timestamp_str() + "]" + (text) + "\n");
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
            ReportManager.reportError("[LoggingError]" + e.getMessage());
        }
    }

    public static void logError(String error) {
        LogManager.log("[Error]" + error);
        ReportManager.reportError(error);
    }

    public static void logInfo(String text) {
//        LogManager.log("[Info]" + text);
    }

    public static void logDebugInfo(String text) {
        if (Utils.configurationBean.debug)
            LogManager.log("[DebugInfo]" + text);
    }

    public static void logPriorityInfo(String text) {
        LogManager.log("[Info]" + text);
    }

    public void logCacheInit() {
        logInfo("[Cache System Initializing]");
    }
}
