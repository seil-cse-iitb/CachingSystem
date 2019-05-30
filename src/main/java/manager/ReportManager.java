package manager;

import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.net.URLEncoder;
import java.util.Properties;

public class ReportManager {
	public static void report(String subject, String text) {
		try {
			String report_reciever_email = PropertiesManager.getProperties().REPORT_RECEIVER_EMAIL;
			String url = "http://10.129.149.9:8080/meta/mail/?to=" + URLEncoder.encode(report_reciever_email) + "&body=" + URLEncoder.encode(text) + "&subject=" + URLEncoder.encode(subject);
			UtilsManager.makeGetRequest(url);
//			LogManager.logInfo("[Report]Report Sent=> Subject: "+subject);
		} catch (Exception e) {
			e.printStackTrace();
//			LogManager.logInfo("[Report][ReportSendingError]"+e.getMessage());
		}
	}

	public static void reportError(String text) {
		String scriptIdentityText = PropertiesManager.getProperties().APP_NAME;
		if(PropertiesManager.getProperties().REPORT_ERROR) {
			ReportManager.report(scriptIdentityText, "[Error]" + text);
		}
	}

	public static void reportInfo(String text) {
		String scriptIdentityText = PropertiesManager.getProperties().APP_NAME;
		ReportManager.report(scriptIdentityText, "[Info]" + text);
	}
}
