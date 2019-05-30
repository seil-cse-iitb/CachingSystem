package version1.manager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class LogManager {
    public final static String logFilePath = PropertiesManager.getProperties().LOG_FILE_PATH;

    public static void log(String text) {
        System.out.println("[" + UtilsManager.current_timestamp_str() + "]" + (text));
        File logFile = new File(LogManager.logFilePath);
        try {
            FileWriter fw = new FileWriter(logFile, true);
            fw.append("[" + UtilsManager.current_timestamp_str() + "]" + (text) + "\n");
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
        LogManager.log("[Info]" + text);
    }

    public static void logDebugInfo(String text) {
        if (PropertiesManager.getProperties().DEBUG)
            LogManager.log("[DebugInfo]" + text);
    }
}
