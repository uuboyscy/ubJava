package uuboy.scy.util;

import uuboy.scy.config.HiveConfiguration;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

import java.util.HashMap;
import java.util.Map;

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

    /** Print query result */
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

    /** Get query result set */
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

        return stmt.executeQuery(sql);
    }

    /** Create Hive specific database */
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

    /** Create Hive database according to constructor */
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

    /** Create Hive table */
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

    /** Drop database */
    public void dropDatabases(String dbName) throws SQLException{
        if (dbName.equals(this.db)){
            try {
                Class.forName(driverName);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }
            Connection con = DriverManager.getConnection(uri, user, passwd);
            Statement stmt = con.createStatement();

            stmt.execute("drop database if exists `" + dbName + "`");
            System.out.println("Database " + dbName + " dropped.");
            con.close();
        }
    }

    /** Drop table */
    public void dropTable(String dbName, String tableName) throws SQLException{
        if (dbName.equals(this.db)){
            try {
                Class.forName(driverName);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }
            Connection con = DriverManager.getConnection(uri, user, passwd);
            Statement stmt = con.createStatement();

            stmt.execute(String.format("drop table if exists `%s`.`%s`", dbName, tableName));
            System.out.println("Table " + tableName + " dropped.");
            con.close();
        }
    }
    
    /** Add partition */
    public void addPartition(String dbName, String tableName, String partitionLocation, Map<String, String> partitioNameValue) throws SQLException {
        if (dbName.equals(this.db)){
            try {
                Class.forName(driverName);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }
            Connection con = DriverManager.getConnection(uri, user, passwd);
            Statement stmt = con.createStatement();

            String partition = "";
            for (String column : partitioNameValue.keySet())
                partition += String.format("%s='%s',", column, partitioNameValue.get(column));
            String sql = String.format("ALTER TABLE %s.%s ADD PARTITION (", dbName, tableName);
            sql += partition.substring(0, partition.length() - 1);
            sql += String.format(") LOCATION '%s'", partitionLocation);

            stmt.execute(sql);
            System.out.println("SQL \"" + sql + "\" executed.");
            con.close();
        }
    }

    public static void main(String[] args) throws SQLException{
        String sql = "SELECT * FROM click LIMIT 10";
        (new HiveOperation("default")).showQuery(sql);
        (new HiveOperation("default")).createHiveDatabase();
        Map<String, String> nk = new HashMap<>();
        nk.put("m", "2020-07");
        nk.put("d", "2020-07-08");
        (new HiveOperation("default")).addPartition(
                "default",
                "tmptable",
                "/hive/default/tmptable/m=2020-07/d=2020-07-08/",
                nk);
    }
}
