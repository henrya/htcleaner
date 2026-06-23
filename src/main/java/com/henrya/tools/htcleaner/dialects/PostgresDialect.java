package com.henrya.tools.htcleaner.dialects;

import com.henrya.tools.htcleaner.constants.ProcessorConstants;
import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.model.KeyRow;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PostgresDialect implements DatabaseDialect {
  @Override
  public List<String> getPrimaryKeys(@Nonnull Connection conn, @Nonnull String table) throws DataException {
    String sql = "SELECT a.attname " +
        "FROM pg_index i " +
        "JOIN pg_class c ON c.oid = i.indrelid " +
        "JOIN pg_namespace n ON n.oid = c.relnamespace " +
        "JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true " +
        "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum " +
        "WHERE n.nspname = ? " +
        "AND c.relname = ? " +
        "AND c.relkind IN ('r', 'p') " +
        "AND i.indisprimary " +
        "ORDER BY k.ord";
    try {
      for (String schemaName : metadataSchemaCandidates(conn, table)) {
        for (String tableName : metadataTableCandidates(table)) {
          List<String> primaryKeys = queryPrimaryKeys(conn, sql, schemaName, tableName);
          if (!primaryKeys.isEmpty()) {
            return primaryKeys;
          }
        }
      }
      return Collections.emptyList();
    } catch (SQLException e) {
      throw new DataException(String.format("Cannot find primary keys from the table %s: %s", table, e), e);
    }
  }

  @Override
  public List<KeyRow> getRecords(@Nonnull Connection conn, @Nonnull String table,
      @Nonnull List<String> primaryKeys, String where, int limit) throws DataException {
    List<KeyRow> records = new ArrayList<>();
    String keyColumns = quotedColumns(primaryKeys);
    String sql = String.format("SELECT %s FROM %s %s ORDER BY %s LIMIT %d",
        keyColumns, quoteIdentifier(table), (where != null ? "WHERE " + where : ""), keyColumns, limit);
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          List<Object> values = new ArrayList<>();
          for (String primaryKey : primaryKeys) {
            values.add(rs.getObject(primaryKey));
          }
          records.add(new KeyRow(primaryKeys, values));
        }
      }
    } catch (SQLException e) {
      throw new DataException(String.format("Cannot get records, table %s: %s", table, e), e);
    }
    return records;
  }

  @Override
  public String getConnectionURI() {
    return ProcessorConstants.CONN_URI_POSTGRES;
  }

  @Override
  public boolean restrictUnqualifiedTablesToCurrentSchema() {
    return true;
  }

  @Override
  public String[] metadataTableTypes() {
    return new String[]{"TABLE", "PARTITIONED TABLE"};
  }

  private List<String> queryPrimaryKeys(Connection conn, String sql, String schemaName, String tableName)
      throws SQLException {
    List<String> pkColumns = new ArrayList<>();
    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, schemaName);
      stmt.setString(2, tableName);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          pkColumns.add(rs.getString(1));
        }
      }
    }
    return pkColumns;
  }
}
