package uuboy.scy.util;

import uuboy.scy.config.HiveConfiguration;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveOperation {
    private String driverName = "org.apache.hive.jdbc.HiveDriver";
    private String host = HiveConfiguration.host;
    private String db = HiveConfiguration.db;
    private String uri = HiveConfiguration.uri;
    private String user = HiveConfiguration.user;
    private String passwd = HiveConfiguration.passwd;

    public HiveOperation(){ }
    public HiveOperation(String db){
        this.db = db;
        this.uri = this.host + "/" + db;
    }
    public HiveOperation(String host, String db){
        this.host = host;
        this.db = db;
        this.uri = host + "/" + db;
    }

    public void showQuery(String sql) throws SQLException{
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection(uri, user, passwd);
        Statement stmt = con.createStatement();

        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            int c = 1;
            while (true){
                try{
                    System.out.print(res.getString(c) + "\t");
                    c++;
                }catch (ArrayIndexOutOfBoundsException e){
                    e.printStackTrace();
                }catch (SQLException e){
                    break;
                }
            }
            System.out.println();
        }
        con.close();
    }

    public ResultSet getQuery(String sql) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection(uri, user, passwd);
        Statement stmt = con.createStatement();

        System.out.println("Running: " + sql);

        ResultSet result = stmt.executeQuery(sql);

        return result;
    }

    public void createHiveDatabase(String dbName) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection(uri, user, passwd);
        Statement stmt = con.createStatement();

        stmt.execute("CREATE DATABASE " + dbName);
        System.out.println("Database " + dbName + " created successfully.");

        con.close();
    }

    public void createHiveDatabase() throws SQLException {
        String dbName = this.db;
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection(uri, user, passwd);
        Statement stmt = con.createStatement();

        stmt.execute("CREATE DATABASE " + dbName);
        System.out.println("Database " + dbName + " created successfully.");

        con.close();
    }

    public void createHiveTable(String sql) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection(uri, user, passwd);
        Statement stmt = con.createStatement();

        stmt.execute(sql);
        System.out.println("Table created successfully.");

        con.close();
    }

    public static void main(String[] args) throws SQLException{
        String sql = "SELECT * FROM click LIMIT 10";
        (new HiveOperation("default")).showQuery(sql);
        (new HiveOperation("default")).createHiveDatabase();
    }
}
