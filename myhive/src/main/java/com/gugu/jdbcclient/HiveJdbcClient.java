package com.gugu.jdbcclient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author gugu
 * @Classname HiveJdbcClient
 * @Description TODO
 * @Date 2019/11/30 18:46
 */
public class HiveJdbcClient {
    public static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args) throws Exception {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        Connection conn = DriverManager.getConnection("jdbc:hive2://master:10000/mydb2","gugu","");
        Statement stat = conn.createStatement();
        String sql = "select * from t_a limit 5";
        ResultSet rs = stat.executeQuery(sql);
        while(rs.next()){
            System.out.println(rs.getString(0) + "-"+ rs.getString(1));
        }


    }

}
