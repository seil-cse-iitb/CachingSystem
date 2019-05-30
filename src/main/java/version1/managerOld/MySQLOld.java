package version1.managerOld;

import java.util.Properties;

public class MySQLOld {

    private final String host;
    private final String user;
    private final String password;
    private final String database;
    private Properties properties;


    public MySQLOld(String host, String user, String password, String database) {
        this.host = host;
        this.user = user;
        this.password = password;
        this.database = database;
        this.setProperties();
    }

    public String getURL() {
        return "jdbc:mysql://" + host + ":3306/" + database + "?useSSL=false&autoReconnect=true&failOverReadOnly=false&maxReconnects=10&useUnicode=true&characterEncoding=UTF-8";
    }

    public String getURL(String database) {
        return "jdbc:mysql://" + host + ":3306/" + database + "?useSSL=false&autoReconnect=true&failOverReadOnly=false&maxReconnects=10&useUnicode=true&characterEncoding=UTF-8";
    }

    public void setProperties() {
        properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");
    }

    public Properties getProperties() {
        return this.properties;
    }
}
