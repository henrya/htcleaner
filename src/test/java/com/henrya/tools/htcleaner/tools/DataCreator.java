package com.henrya.tools.htcleaner.tools;

import com.henrya.tools.htcleaner.constants.ProcessorConstants;
import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public final class DataCreator {
  private static final String USER = "sa";
  private static final String PASSWORD = "";

  public static void createData(String table, int rows) {
    try (Connection conn = DriverManager.getConnection(ProcessorConstants.CONN_URI_H2, USER, PASSWORD);
         Statement stmt = conn.createStatement()) {
      String sql = "CREATE TABLE IF NOT EXISTS " + table +
          "(id INTEGER not NULL, " +
          " a VARCHAR(255), " +
          " b VARCHAR(255), " +
          " c INTEGER, " +
          " PRIMARY KEY ( id ))";
      stmt.executeUpdate(sql);
      if (rows > 0) {
        for (int i = 0; i < rows; i++) {
          sql = "INSERT INTO " + table + " VALUES (" + (i + 1) + ",'a" + i + "', 'b" + i + "'," + i + ")";
          stmt.executeUpdate(sql);
        }
      }

    } catch (SQLException se) {
      throw new IllegalStateException(se);
    }
  }

  public static void createDataWithoutPK(String table, int rows) {
    try (Connection conn = DriverManager.getConnection(ProcessorConstants.CONN_URI_H2, USER, PASSWORD);
         Statement stmt = conn.createStatement()) {
      String sql = "CREATE TABLE IF NOT EXISTS " + table +
          "(id INTEGER not NULL, " +
          " a VARCHAR(255), " +
          " b VARCHAR(255), " +
          " c INTEGER) ";
      stmt.executeUpdate(sql);
      if (rows > 0) {
        for (int i = 0; i < rows; i++) {
          sql = "INSERT INTO " + table + " VALUES (" + (i + 1) + ",'a" + i + "', 'b" + i + "'," + i + ")";
          stmt.executeUpdate(sql);
        }
      }

    } catch (SQLException se) {
      throw new IllegalStateException(se);
    }
  }

  public static void createCompositeData(String table, int rows) {
    try (Connection conn = DriverManager.getConnection(ProcessorConstants.CONN_URI_H2, USER, PASSWORD);
         Statement stmt = conn.createStatement()) {
      String sql = "CREATE TABLE IF NOT EXISTS " + table +
          "(tenant_id INTEGER not NULL, " +
          " id INTEGER not NULL, " +
          " a VARCHAR(255), " +
          " b VARCHAR(255), " +
          " c INTEGER, " +
          " PRIMARY KEY ( tenant_id, id ))";
      stmt.executeUpdate(sql);
      if (rows > 0) {
        for (int i = 0; i < rows; i++) {
          int tenantId = (i % 2) + 1;
          int id = (i / 2) + 1;
          sql = "INSERT INTO " + table + " VALUES (" + tenantId + "," + id + ",'a" + i + "', 'b" + i + "',"
              + i + ")";
          stmt.executeUpdate(sql);
        }
      }
    } catch (SQLException se) {
      throw new IllegalStateException(se);
    }
  }

  public static void createStringKeyData(String table, String... keys) {
    try (Connection conn = DriverManager.getConnection(ProcessorConstants.CONN_URI_H2, USER, PASSWORD);
         Statement stmt = conn.createStatement()) {
      String sql = "CREATE TABLE IF NOT EXISTS " + table +
          "(id VARCHAR(255) not NULL, " +
          " a VARCHAR(255), " +
          " PRIMARY KEY ( id ))";
      stmt.executeUpdate(sql);
      for (String key : keys) {
        sql = "INSERT INTO " + table + " VALUES ('" + sqlLiteral(key) + "','value')";
        stmt.executeUpdate(sql);
      }
    } catch (SQLException se) {
      throw new IllegalStateException(se);
    }
  }

  public static void executeUpdate(String sql) {
    try (Connection conn = DriverManager.getConnection(ProcessorConstants.CONN_URI_H2, USER, PASSWORD);
         Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(sql);
    } catch (SQLException se) {
      throw new IllegalStateException(se);
    }
  }

  public static int executeCount(@Nonnull String table, String where) {
    String filter = (where != null) ? " WHERE " + where : "";
    try (Connection conn = DriverManager.getConnection(ProcessorConstants.CONN_URI_H2, USER, PASSWORD);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(String.format("SELECT COUNT(1) as ROWS FROM %s%s", table, filter))) {
      rs.next();
      return rs.getInt("ROWS");
    } catch (SQLException se) {
      throw new IllegalStateException(se);
    }
  }

  private static String sqlLiteral(String value) {
    return value.replace("'", "''");
  }

  private DataCreator() {
    throw new UnsupportedOperationException("This class cannot be initialized directly");
  }
}
