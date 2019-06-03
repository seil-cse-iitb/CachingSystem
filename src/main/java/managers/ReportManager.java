package managers;

import java.net.URLEncoder;

import static managers.Utils.configurationBean;

public class ReportManager {
	public static void report(String subject, String text) {
		try {
			String reportReceiverEmail = configurationBean.reportReceiverEmail;
			String url = "http://10.129.149.9:8080/meta/mail/?to=" + URLEncoder.encode(reportReceiverEmail) + "&body=" + URLEncoder.encode(text) + "&subject=" + URLEncoder.encode(subject);
			Utils.makeGetRequest(url);
//			LogManager.logInfo("[Report]Report Sent=> Subject: "+subject);
		} catch (Exception e) {
			e.printStackTrace();
//			LogManager.logInfo("[Report][ReportSendingError]"+e.getMessage());
		}
	}

	public static void reportError(String text) {
		String scriptIdentityText = configurationBean.appName;
		if(configurationBean.reportError) {
			ReportManager.report(scriptIdentityText, "[Error]" + text);
		}
	}

	public static void reportInfo(String text) {
		String scriptIdentityText = configurationBean.appName;
		ReportManager.report(scriptIdentityText, "[Info]" + text);
	}
}
