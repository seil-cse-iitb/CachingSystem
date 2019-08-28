package controllers;

import beans.ConfigurationBean;
import beans.DatabaseBean;

import java.util.Properties;

public class DatabaseController {
    public DatabaseController(CacheSystemController cb) {
    }

    public Properties getProperties(DatabaseBean db) {
        Properties properties = new Properties();
        properties.setProperty("user", db.getUsername());
        properties.setProperty("password", db.getPassword());
//        if (db.getDatabaseType().equalsIgnoreCase("postgres")) {
//            properties.setProperty("driver","org.postgresql.Driver");
//        } else {
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");
//        }
        return properties;
    }

    public String getURL(DatabaseBean db) {
//        if (db.getDatabaseType().equalsIgnoreCase("postgres")) {
//            return "jdbc:postgresql://" + db.getHost() + ":" + db.getPort() + "/" + db.getDatabaseName();
//        } else {
        return "jdbc:mysql://" + db.getHost() + ":" + db.getPort() + "/" + db.getDatabaseName() + "?useSSL=false&autoReconnect=true&failOverReadOnly=false&maxReconnects=10&useUnicode=true&characterEncoding=UTF-8";
//        }
    }

    public String getURL(DatabaseBean db, String databaseName) {
//        if (db.getDatabaseType().equalsIgnoreCase("postgres")) {
//            return "jdbc:postgresql://" + db.getHost() + ":" + db.getPort() + "/" + databaseName;
//        } else {
        return "jdbc:mysql://" + db.getHost() + ":" + db.getPort() + "/" + databaseName + "?useSSL=false&autoReconnect=true&failOverReadOnly=false&maxReconnects=10&useUnicode=true&characterEncoding=UTF-8";
//        }
    }
}
