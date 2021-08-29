package uuboy.scy.config;

public class HiveConfiguration {
    public static String host = "jdbc:hive2://192.168.1.168:10000";
    public static String db = "default";
    public static String uri = host + "/" + db;
    public static String user = "hive";
    public static String passwd = "";
}
