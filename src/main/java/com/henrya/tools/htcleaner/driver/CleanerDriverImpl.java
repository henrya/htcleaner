package com.henrya.tools.htcleaner.driver;

import com.henrya.tools.htcleaner.constants.*;
import com.henrya.tools.htcleaner.exception.CleanerException;
import com.henrya.tools.htcleaner.exception.DataException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The JDBC driver class implementing the low level logic for the executor
 */
public class CleanerDriverImpl implements CleanerDriver {

  /**
   * Connection instance
   */
  Connection conn = null;

  /**
   * Driver name
   */
  String driver;

  /**
   * Cleaner driver constructor
   *
   * @param driver Driver name
   */
  public CleanerDriverImpl(String driver){
    this.driver = driver;
  }

  /**
   * Will connect to the database
   *
   * @param host host name
   * @param port port
   * @param database database/schema name
   * @param user user name
   * @param password password
   * @throws DataException Exception to be thrown
   */
  @Override
  public void connect(@Nonnull String host, @Nonnull Integer port, @Nonnull String database, @Nonnull String user, @Nonnull String password)  throws DataException {
    try {
      this.conn = DriverManager.getConnection(String.format(getConnectionURI(driver), host, port, database), user, password);
    } catch (SQLException | CleanerException e) {
      throw new DataException(String.format("Failed to connect: %s" , e.getMessage()));
    }
  }

  /**
   * Detects primary keys for the specified table
   *
   * @param table table name
   * @return List<String> of primary keys
   * @throws DataException  Exception to be thrown
   */
  @Override
  public List<String> getPrimaryKeys(@Nonnull String table) throws DataException {
    List<String> pkColumns = new ArrayList<>();
    try (Statement stmt = conn.createStatement()) {
      // fetch column information in the table
      ResultSet rs = stmt.executeQuery("SHOW COLUMNS FROM " + table);
      while (rs.next()) {
        // the KEY column should be specified as PRIMARY
        if (rs.getString("KEY").equals("PRI")) {
          pkColumns.add(rs.getString("FIELD"));
          // we will support only one primary key
          break;
        }
      }
    } catch (SQLException e) {
      throw new DataException(String.format("Cannot find primary keys from the table %s: %s", table, e));
    }
    return pkColumns;
  }

  /**
   * Fetch record by primary key
   * @param table table name
   * @param primaryKey primary key
   * @param where optional where statement
   * @param limit chunk size
   * @return List<String> List of records
   * @throws DataException Exception thrown
   */
  @Override
  public List<String> getRecords(@Nonnull String table,@Nonnull String primaryKey, String where, int limit) throws DataException {
    List<String> records = new ArrayList<>();
    try (Statement stmt = conn.createStatement()) {
      String sql = String.format("SELECT %s FROM %s %s LIMIT %d", primaryKey, table,  (where != null) ? " WHERE "+where:"" , limit);
      ResultSet rs = stmt.executeQuery(sql);
      while (rs.next()) {
        records.add(rs.getString(primaryKey));
      }
    } catch (SQLException e) {
      throw new DataException(String.format("Cannot get records, table %s: %s", table, e));
    }
    return records;
  }

  /**
   * Removes records from the table by primary key
   *
   * @param table Table name
   * @param primaryKey Primary key
   * @param where Where statement
   * @param keys List of keys to be removed
   * @param doCommit Whether to commit operation or not
   * @return int Amount of rows removed
   * @throws DataException Exception thrown
   */
  @Override
  public int deleteRecords(@Nonnull String table, @Nonnull String primaryKey, String where,@Nonnull List<String> keys, boolean doCommit) throws DataException{
    try (Statement stmt = conn.createStatement()) {
      conn.setAutoCommit(false);
      String sql = String.format("DELETE FROM %s WHERE %s IN ('%s') %s", table, primaryKey, String.join(
          "','", keys), (where != null) ? " AND "+where:"");
      return stmt.executeUpdate(sql);
    } catch (SQLException e) {
      throw new DataException(String.format("Cannot delete records, table %s: %s", table, e));
    } finally {
      try {
        // do not commit in dry run mode
        conn.setAutoCommit(doCommit);
      } catch (SQLException e) {
        Logger.getGlobal().log(Level.SEVERE, () ->
                String.format("Failed to commit, failed with an exception: %s", e.getMessage())
        );
      }
    }
  }

  /**
   * Get table metadata
   *
   * @param table Table name
   * @return Map<String,String> Table metadata
   * @throws DataException Exception thrown
   */
  @Override
  public Map<String,String> getTable(@Nonnull String table) throws DataException {
    Map<String,String> tableInfo = new HashMap<>();
    try {
      DatabaseMetaData meta = conn.getMetaData();
      ResultSet resultSet = meta.getTables(null, null, table.toUpperCase(), new String[] {"TABLE"});
      while (resultSet.next()) {
        tableInfo.put("name",resultSet.getString("TABLE_NAME"));
        tableInfo.put("schema",resultSet.getString("TABLE_SCHEM"));
        tableInfo.put("catalog",resultSet.getString("TABLE_CAT"));
        tableInfo.put("type",resultSet.getString("TABLE_TYPE"));
      }
      return tableInfo;
    } catch (SQLException e){
      throw new DataException(String.format("Cannot get table: %s", table));
    }
  }

  /**
   * Count amount of the rows in the table
   *
   * @param table Table name
   * @param where Where statement
   * @return int Amount of rows
   * @throws DataException Exception thrown
   */
  @Override
  public int countRows(@Nonnull String table, String where) throws DataException {
    try (Statement stmt = conn.createStatement()) {
      String sql = String.format("SELECT COUNT(1) as ROWS FROM %s %s", table,  (where != null) ? " WHERE "+where:"");
      ResultSet rs = stmt.executeQuery(sql);
      rs.next();
      return rs.getInt("ROWS");
    } catch (SQLException e) {
      throw new DataException(String.format("Execution failed, table %s: %s", table, e));
    }
  }

  /**
   * Will return a connection IRL for specific driver
   *
   * @param driver driver name
   * @return connection URL
   * @throws CleanerException Exception to be thrown
   */
  private String getConnectionURI(@Nonnull String driver) throws CleanerException {
    switch (driver){
      case "mysql":
        return ProcessorConstants.CONN_URI_MYSQL;
      case "h2":
        return ProcessorConstants.CONN_URI_H2;
      default:
        throw new CleanerException("Unexpected driver: " + driver);
    }
  }

  /**
   * Get the current connection
   *
   * @return Connection connection
   */
  public Connection getConn(){
    return conn;
  }

  /**
   * Will set a connection instance
   *
   * @param conn Connection
   */
  public void setConn(Connection conn){
    this.conn = conn;
  }
}
