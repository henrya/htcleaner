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
import java.util.List;

public class H2Dialect implements DatabaseDialect {
  @Override
  public List<String> getPrimaryKeys(@Nonnull Connection conn, @Nonnull String table) throws DataException {
    List<String> pkColumns = new ArrayList<>();
    try {
      try (ResultSet tables = conn.getMetaData().getTables(metadataCatalogName(table), metadataSchemaName(table),
          metadataTableName(table), null)) {
        if (!tables.next()) {
          throw new DataException("Table not found: " + table);
        }
      }

      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("SHOW COLUMNS FROM " + quoteIdentifier(table))) {
          while (rs.next()) {
            if (rs.getString("KEY") != null && rs.getString("KEY").equals("PRI")) {
              pkColumns.add(rs.getString("FIELD"));
            }
          }
        }
      }
    } catch (SQLException e) {
      throw new DataException(String.format("Error getting primary keys for table %s: %s", table, e.getMessage()), e);
    }
    return pkColumns;
  }

  @Override
  public List<KeyRow> getRecords(@Nonnull Connection conn, @Nonnull String table, @Nonnull List<String> primaryKeys, String where, int limit) throws DataException {
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
    return ProcessorConstants.CONN_URI_H2;
  }

  @Override
  public String quoteIdentifier(@Nonnull String identifier) throws DataException {
    return SqlIdentifier.quoteQualified(identifier, "\"", true);
  }

  @Override
  public String metadataTableName(@Nonnull String table) throws DataException {
    return SqlIdentifier.normalizeSimple(table, true);
  }

  @Override
  public String metadataSchemaName(@Nonnull String table) throws DataException {
    return SqlIdentifier.qualifier(table, true);
  }
}
