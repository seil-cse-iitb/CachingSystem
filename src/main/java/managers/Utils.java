package managers;

import beans.ConfigurationBean;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {
    public static ConfigurationBean configurationBean=null;
    public static DateFormat df = new SimpleDateFormat("dd/MM/yyyy");//TODO make it for time as well
    public static long getTimeInSec(Date date) {
        return date.getTime() / 1000;
    }
    public static void sparkLogsOff() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }
    public static String current_timestamp_str() {
        return new Date().toString();
    }

    public static String makeGetRequest(String url) {
        String responseStr = "";
        try {
            URL urlObj = new URL(url);
            URLConnection urlConnection = null;
            urlConnection = urlObj.openConnection();
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(
                            urlConnection.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null)
                responseStr += inputLine;
            in.close();
        } catch (IOException e) {
            LogManager.logInfo("[GET REQUEST(" + url + ")]" + e.getMessage());
        }
        return responseStr;
    }

}
