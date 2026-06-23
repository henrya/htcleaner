package com.henrya.tools.htcleaner.driver;

import com.henrya.tools.htcleaner.dialects.DatabaseDialect;
import com.henrya.tools.htcleaner.dialects.DialectFactory;
import com.henrya.tools.htcleaner.exception.CleanerException;
import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.model.KeyRow;
import com.henrya.tools.htcleaner.model.TableMetadata;
import com.henrya.tools.htcleaner.sql.SqlPredicate;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * JDBC-backed cleaner driver.
 */
public final class CleanerDriverImpl implements CleanerDriver, AutoCloseable {

  /**
   * Connection instance
   */
  private Connection conn = null;

  /**
   * Database dialect
   */
  private final DatabaseDialect dialect;

  /**
   * Cleaner driver constructor
   *
   * @param driver Driver name
   */
  public CleanerDriverImpl(String driver) throws CleanerException {
    this.dialect = DialectFactory.createDialect(driver);
  }

  /**
   * Opens a JDBC connection.
   *
   * @param host     host name
   * @param port     port
   * @param database database/schema name
   * @param user     user name
   * @param password optional password
   * @throws DataException when the connection cannot be opened
   */
  @Override
  public void connect(@Nonnull String host, @Nonnull Integer port, @Nonnull String database, @Nonnull String user,
      String password) throws DataException {
    try {
      this.conn = DriverManager.getConnection(String.format(dialect.getConnectionURI(), host, port, database), user,
          password);
    } catch (SQLException e) {
      throw new DataException(String.format("Failed to connect: %s", e.getMessage()), e);
    }
  }

  /**
   * Detects primary keys for the specified table.
   *
   * @param table table name
   * @return List<String> of primary keys
   * @throws DataException when primary-key metadata cannot be read
   */
  @Override
  public List<String> getPrimaryKeys(@Nonnull String table) throws DataException {
    return dialect.getPrimaryKeys(conn, table);
  }

  /**
   * Fetch records by primary key.
   *
   * @param table      table name
   * @param primaryKeys primary keys
   * @param where      optional where statement
   * @param limit      chunk size
   * @return List<KeyRow> List of records
   * @throws DataException Exception thrown
   */
  @Override
  public List<KeyRow> getRecords(@Nonnull String table, @Nonnull List<String> primaryKeys, String where, int limit)
      throws DataException {
    return dialect.getRecords(conn, table, primaryKeys, normalizeWhere(where), limit);
  }

  /**
   * Removes records from the table by primary key.
   *
   * @param table      Table name
   * @param primaryKeys Primary keys
   * @param where      Where statement
   * @param keys       List of keys to be removed
   * @param doCommit   Whether to commit operation or not
   * @return int Amount of rows removed
   * @throws DataException Exception thrown
   */
  @Override
  public int deleteRecords(@Nonnull String table, @Nonnull List<String> primaryKeys, String where,
      @Nonnull List<KeyRow> keys, boolean doCommit) throws DataException {
    if (keys.isEmpty()) {
      return 0;
    }

    boolean originalAutoCommit = true;
    try {
      originalAutoCommit = conn.getAutoCommit();
      conn.setAutoCommit(false);

      String sql = dialect.buildDeleteSql(table, primaryKeys, normalizeWhere(where), keys);
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        bindKeys(stmt, primaryKeys, keys);
        int updates = stmt.executeUpdate();
        if (doCommit) {
          conn.commit();
        } else {
          conn.rollback();
        }
        return updates;
      }
    } catch (SQLException e) {
      rollbackQuietly();
      throw new DataException(String.format("Cannot delete records, table %s: %s", table, e), e);
    } finally {
      try {
        conn.setAutoCommit(originalAutoCommit);
      } catch (SQLException e) {
        Logger.getGlobal().log(Level.SEVERE, "Failed to restore auto-commit", e);
      }
    }
  }

  /**
   * Get table metadata
   *
   * @param table Table name
   * @return table metadata when the table exists
   * @throws DataException Exception thrown
   */
  @Override
  public Optional<TableMetadata> getTable(@Nonnull String table) throws DataException {
    return dialect.getTable(conn, table);
  }

  /**
   * Counts rows in the table.
   *
   * @param table Table name
   * @param where Where statement
   * @return int Amount of rows
   * @throws DataException Exception thrown
   */
  @Override
  public int countRows(@Nonnull String table, String where) throws DataException {
    try (Statement stmt = conn.createStatement()) {
      String normalizedWhere = normalizeWhere(where);
      String sql = String.format("SELECT COUNT(1) as ROWS FROM %s %s",
          dialect.quoteIdentifier(table), (normalizedWhere != null) ? " WHERE " + normalizedWhere : "");
      try (ResultSet rs = stmt.executeQuery(sql)) {
        rs.next();
        return rs.getInt("ROWS");
      }
    } catch (SQLException e) {
      throw new DataException(String.format("Execution failed, table %s: %s", table, e), e);
    }
  }

  /**
   * Get the current connection
   *
   * @return Connection connection
   */
  Connection getConn() {
    return conn;
  }

  /**
   * Will set a connection instance
   *
   * @param conn Connection
   */
  void setConn(Connection conn) {
    this.conn = conn;
  }

  @Override
  public boolean isConnected() {
    try {
      return conn != null && !conn.isClosed();
    } catch (SQLException e) {
      return false;
    }
  }

  /**
   * Closes the connection
   */
  @Override
  public void close() {
    try {
      if (conn != null) {
        conn.close();
      }
    } catch (SQLException e) {
      Logger.getGlobal().log(Level.SEVERE, "Failed to close the connection", e);
    } finally {
      conn = null;
    }
  }

  private void bindKeys(PreparedStatement stmt, List<String> primaryKeys, List<KeyRow> keys)
      throws SQLException, DataException {
    int index = 1;
    for (KeyRow key : keys) {
      validateKeyRow(primaryKeys, key);
      for (Object value : key.getValues()) {
        stmt.setObject(index++, value);
      }
    }
  }

  private void validateKeyRow(List<String> primaryKeys, KeyRow key) throws DataException {
    if (!key.getColumns().equals(primaryKeys)) {
      throw new DataException("Fetched key row does not match primary key metadata");
    }
  }

  private String normalizeWhere(String where) throws DataException {
    return SqlPredicate.normalize(where);
  }

  private void rollbackQuietly() {
    if (conn == null) {
      return;
    }
    try {
      conn.rollback();
    } catch (SQLException e) {
      Logger.getGlobal().log(Level.SEVERE, "Rollback failed", e);
    }
  }
}
