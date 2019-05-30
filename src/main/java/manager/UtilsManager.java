package manager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class UtilsManager {
    public final static Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("Asia/Kolkata"));

    public static String current_timestamp_str() {
        return new Date().toString();
    }

    public static String makeGetRequest(String urlStr) {
        String responseStr = "";
        try {
            URL urlObj = new URL(urlStr);
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
            LogManager.logError("[GET REQUEST(" + urlStr + ")]" + e.getMessage());
        }
        return responseStr;
    }


}
