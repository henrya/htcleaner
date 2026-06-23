package com.henrya.tools.htcleaner.dialects;

import com.henrya.tools.htcleaner.constants.ProcessorConstants;
import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.model.KeyRow;
import com.henrya.tools.htcleaner.sql.SqlIdentifier;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class OracleDialect implements DatabaseDialect {
  private static final int MAX_IN_LIST_EXPRESSIONS = 1_000;

  @Override
  public List<String> getPrimaryKeys(@Nonnull Connection conn, @Nonnull String table) throws DataException {
    try {
      DatabaseMetaData metaData = conn.getMetaData();
      for (String schemaName : metadataSchemaCandidates(conn, table)) {
        for (String tableName : metadataTableCandidates(table)) {
          List<String> primaryKeys = queryPrimaryKeys(metaData, schemaName, tableName);
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
    String innerWhere = (where != null) ? " WHERE " + where : "";
    String keyColumns = quotedColumns(primaryKeys);
    String sql = String.format("SELECT %s FROM (SELECT %s FROM %s%s ORDER BY %s) WHERE ROWNUM <= %d",
        keyColumns, keyColumns, quoteIdentifier(table), innerWhere, keyColumns, limit);

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
    return ProcessorConstants.CONN_URI_ORACLE;
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

  @Override
  public boolean restrictUnqualifiedTablesToCurrentSchema() {
    return true;
  }

  @Override
  public String buildDeleteSql(String table, List<String> primaryKeys, String where, List<KeyRow> keys)
      throws DataException {
    if (primaryKeys.size() == 1 && keys.size() > MAX_IN_LIST_EXPRESSIONS) {
      throw new DataException(String.format(
          "Oracle delete batch requires %d IN-list expressions, maximum is %d. Reduce --limit.",
          keys.size(), MAX_IN_LIST_EXPRESSIONS));
    }
    return DatabaseDialect.super.buildDeleteSql(table, primaryKeys, where, keys);
  }

  private List<String> queryPrimaryKeys(DatabaseMetaData metaData, String schemaName, String tableName)
      throws SQLException {
    List<PrimaryKeyColumn> columns = new ArrayList<>();
    try (ResultSet rs = metaData.getPrimaryKeys(null, schemaName, tableName)) {
      while (rs.next()) {
        columns.add(new PrimaryKeyColumn(rs.getShort("KEY_SEQ"), rs.getString("COLUMN_NAME")));
      }
    }
    return columns.stream()
        .sorted(Comparator.comparingInt(PrimaryKeyColumn::getKeySeq))
        .map(PrimaryKeyColumn::getColumnName)
        .collect(Collectors.toList());
  }

  private static final class PrimaryKeyColumn {
    private final int keySeq;
    private final String columnName;

    private PrimaryKeyColumn(int keySeq, String columnName) {
      this.keySeq = keySeq;
      this.columnName = columnName;
    }

    private int getKeySeq() {
      return keySeq;
    }

    private String getColumnName() {
      return columnName;
    }
  }
}
