package com.henrya.tools.htcleaner.tools;

import com.henrya.tools.htcleaner.constants.ProcessorConstants;
import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DataCreator {
  public static void createData(String table, int rows){
    Connection conn;
    Statement stmt;
    try {
      conn = DriverManager.getConnection(ProcessorConstants.CONN_URI_H2,"sa","");
      stmt = conn.createStatement();
      String sql =  "CREATE TABLE IF NOT EXISTS "+table+
          "(id INTEGER not NULL, " +
          " a VARCHAR(255), " +
          " b VARCHAR(255), " +
          " c INTEGER, " +
          " PRIMARY KEY ( id ))";
      stmt.executeUpdate(sql);
      if(rows > 0){
        for(int i=0; i<rows;i++){
          sql = "INSERT INTO "+table+ " VALUES ("+(i+1)+",'a"+i+"', 'b"+i+"',"+i+")";
          stmt.executeUpdate(sql);
        }
      }

      stmt.close();
      conn.close();
    } catch(SQLException se) {
      se.printStackTrace();
    }
  }

  public static void createDataWithoutPK(String table, int rows){
    Connection conn;
    Statement stmt;
    try {
      conn = DriverManager.getConnection(ProcessorConstants.CONN_URI_H2,"sa","");
      stmt = conn.createStatement();
      String sql =  "CREATE TABLE IF NOT EXISTS "+table+
          "(id INTEGER not NULL, " +
          " a VARCHAR(255), " +
          " b VARCHAR(255), " +
          " c INTEGER) ";
      stmt.executeUpdate(sql);
      if(rows > 0){
        for(int i=0; i<rows;i++){
          sql = "INSERT INTO "+table+ " VALUES ("+(i+1)+",'a"+i+"', 'b"+i+"',"+i+")";
          stmt.executeUpdate(sql);
        }
      }

      stmt.close();
      conn.close();
    } catch(SQLException se) {
      se.printStackTrace();
    }
  }

  public static void executeUpdate(String sql){
    Connection conn;
    Statement stmt;
    try {
      conn = DriverManager.getConnection(ProcessorConstants.CONN_URI_H2,"sa","");
      stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      stmt.close();
      conn.close();
    } catch(SQLException se) {
      se.printStackTrace();
    }
  }

  public static int executeCount(@Nonnull String table, String where) {
    Connection conn;
    Statement stmt;
    int amount = 0;
    try {
      conn = DriverManager.getConnection(ProcessorConstants.CONN_URI_H2,"sa","");
      stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(String.format("SELECT COUNT(1) as ROWS FROM %s %s", table,  (where != null) ? " WHERE "+where:""));
      rs.next();
      amount = rs.getInt("ROWS");
      conn.close();
      stmt.close();
      return amount;
    } catch(SQLException se) {
      se.printStackTrace();
    }
    return amount;
  }
}
