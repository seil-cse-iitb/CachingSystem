package manager;
//http://owner.aeonbits.org/docs/

public interface ConfigManager extends org.aeonbits.owner.Config {
    @DefaultValue("Caching System")
    String appName();
    String driverHostname();

    String cacheMySQLHost();
    String cacheMySQLUser();
    String cacheMySQLPassword();
    String cacheMySQLDatabase();


    @DefaultValue("dummyMainMySQLHost")
    String mainMySQLHost();
    @DefaultValue("dummyUser")
    String mainMySQLUser();
    @DefaultValue("dummyPassword")
    String mainMySQLPassword();
    @DefaultValue("dummyDatabase")
    String mainMySQLDatabase();

}
