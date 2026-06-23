package com.henrya.tools.htcleaner.dialects;

import com.henrya.tools.htcleaner.constants.ProcessorConstants;
import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.model.KeyRow;
import com.henrya.tools.htcleaner.sql.SqlIdentifier;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MySqlDialect implements DatabaseDialect {
  @Override
  public List<String> getPrimaryKeys(@Nonnull Connection conn, @Nonnull String table) throws DataException {
    List<String> pkColumns = new ArrayList<>();
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SHOW COLUMNS FROM " + quoteIdentifier(table))) {
        while (rs.next()) {
          if ("PRI".equals(rs.getString("KEY"))) {
            pkColumns.add(rs.getString("FIELD"));
          }
        }
      }
    } catch (SQLException e) {
      throw new DataException(String.format("Cannot find primary keys from the table %s: %s", table, e), e);
    }
    return pkColumns;
  }

  @Override
  public List<KeyRow> getRecords(@Nonnull Connection conn, @Nonnull String table,
      @Nonnull List<String> primaryKeys, String where, int limit) throws DataException {
    List<KeyRow> records = new ArrayList<>();
    try (Statement stmt = conn.createStatement()) {
      String keyColumns = quotedColumns(primaryKeys);
      String sql = String.format("SELECT %s FROM %s %s ORDER BY %s LIMIT %d",
          keyColumns, quoteIdentifier(table), (where != null) ? " WHERE " + where : "", keyColumns, limit);
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
    return ProcessorConstants.CONN_URI_MYSQL;
  }

  @Override
  public String quoteIdentifier(@Nonnull String identifier) throws DataException {
    return SqlIdentifier.quoteQualified(identifier, "`", false);
  }

  @Override
  public String metadataCatalogName(@Nonnull String table) throws DataException {
    return SqlIdentifier.qualifier(table, false);
  }

  @Override
  public List<String> metadataCatalogCandidates(@Nonnull Connection conn, @Nonnull String table)
      throws SQLException, DataException {
    String catalogName = metadataCatalogName(table);
    if (catalogName != null && !catalogName.isEmpty()) {
      return DatabaseDialect.super.metadataCatalogCandidates(conn, table);
    }
    String currentCatalog = conn.getCatalog();
    if (currentCatalog == null || currentCatalog.trim().isEmpty()) {
      throw new DataException("Cannot resolve current database for unqualified table: " + table);
    }
    return Collections.singletonList(currentCatalog);
  }

  @Override
  public String metadataSchemaName(@Nonnull String table) {
    return null;
  }
}
