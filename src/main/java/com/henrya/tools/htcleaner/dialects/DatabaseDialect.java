package com.henrya.tools.htcleaner.dialects;

import com.henrya.tools.htcleaner.constants.ParameterConstants;
import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.model.KeyRow;
import com.henrya.tools.htcleaner.model.TableMetadata;
import com.henrya.tools.htcleaner.sql.SqlIdentifier;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Database-specific SQL behavior used by the cleaner.
 */
public interface DatabaseDialect {

  /**
   * Returns primary-key column names in database metadata order.
   */
  List<String> getPrimaryKeys(@Nonnull Connection conn, @Nonnull String table) throws DataException;

  /**
   * Fetches the next batch of primary-key values for rows matching the optional predicate.
   */
  List<KeyRow> getRecords(@Nonnull Connection conn, @Nonnull String table, @Nonnull List<String> primaryKeys,
      String where, int limit) throws DataException;

  /**
   * Returns a JDBC connection URI format string with host, port, and database placeholders.
   */
  String getConnectionURI() throws SQLException;

  /**
   * Builds a prepared delete statement for the given primary-key batch.
   */
  default String buildDeleteSql(String table, List<String> primaryKeys, String where, List<KeyRow> keys)
      throws DataException {
    validateDeleteBatch(primaryKeys, keys);

    String sql;
    if (primaryKeys.size() == 1) {
      String placeholders = String.join(", ", Collections.nCopies(keys.size(), "?"));
      sql = String.format("DELETE FROM %s WHERE %s IN (%s)",
          quoteIdentifier(table), quoteIdentifier(primaryKeys.get(0)), placeholders);
    } else {
      sql = buildCompositeDeleteSql(table, primaryKeys, keys);
    }
    return (where != null) ? sql + " AND (" + where + ")" : sql;
  }

  /**
   * Quotes a user-supplied identifier according to dialect rules.
   */
  default String quoteIdentifier(@Nonnull String identifier) throws DataException {
    return SqlIdentifier.quoteQualified(identifier, "\"", false);
  }

  /**
   * Quotes primary-key columns once for generated select/order/delete clauses.
   */
  default String quotedColumns(List<String> primaryKeys) throws DataException {
    StringJoiner quoted = new StringJoiner(", ");
    for (String primaryKey : primaryKeys) {
      quoted.add(quoteIdentifier(primaryKey));
    }
    return quoted.toString();
  }

  /**
   * Converts a CLI table value into the table-name value used for metadata lookup.
   */
  default String metadataTableName(@Nonnull String table) throws DataException {
    return SqlIdentifier.normalizeSimple(table, false);
  }

  /**
   * Converts a CLI table value into the schema-name value used for metadata lookup.
   */
  default String metadataSchemaName(@Nonnull String table) throws DataException {
    return SqlIdentifier.qualifier(table, false);
  }

  /**
   * Returns schema-name candidates for metadata lookup.
   */
  default List<String> metadataSchemaCandidates(@Nonnull Connection conn, @Nonnull String table)
      throws SQLException, DataException {
    String schemaName = metadataSchemaName(table);
    if (schemaName == null && restrictUnqualifiedTablesToCurrentSchema()) {
      schemaName = conn.getSchema();
      if (schemaName == null || schemaName.trim().isEmpty()) {
        throw new DataException("Cannot resolve current schema for unqualified table: " + table);
      }
    }
    return candidates(schemaName);
  }

  /**
   * Returns table-name candidates for metadata lookup.
   */
  default List<String> metadataTableCandidates(@Nonnull String table) throws DataException {
    return candidates(metadataTableName(table));
  }

  /**
   * Converts a CLI table value into the catalog-name value used for metadata lookup.
   */
  default String metadataCatalogName(@Nonnull String table) throws DataException {
    return null;
  }

  /**
   * Returns catalog-name candidates for metadata lookup.
   */
  default List<String> metadataCatalogCandidates(@Nonnull Connection conn, @Nonnull String table)
      throws SQLException, DataException {
    return candidates(metadataCatalogName(table));
  }

  /**
   * Returns table metadata types that can be cleaned by this dialect.
   */
  default String[] metadataTableTypes() {
    return new String[]{"TABLE"};
  }

  /**
   * Whether unqualified table names must resolve inside the connection's current schema.
   */
  default boolean restrictUnqualifiedTablesToCurrentSchema() {
    return false;
  }

  /**
   * Resolves table metadata using exact, upper-case, and lower-case candidates.
   */
  default Optional<TableMetadata> getTable(@Nonnull Connection conn, @Nonnull String table) throws DataException {
    try {
      DatabaseMetaData meta = conn.getMetaData();
      List<String> catalogCandidates = metadataCatalogCandidates(conn, table);
      List<String> schemaCandidates = metadataSchemaCandidates(conn, table);
      List<String> tableCandidates = metadataTableCandidates(table);
      String[] tableTypes = metadataTableTypes();
      for (String catalogCandidate : catalogCandidates) {
        for (String schemaCandidate : schemaCandidates) {
          for (String tableCandidate : tableCandidates) {
            Optional<TableMetadata> tableInfo = readTableInfo(meta, catalogCandidate, schemaCandidate, tableCandidate,
                tableTypes);
            if (tableInfo.isPresent()) {
              return tableInfo;
            }
          }
        }
      }
      return Optional.empty();
    } catch (SQLException e) {
      throw new DataException(String.format("Cannot get table: %s", table), e);
    }
  }

  private String buildCompositeDeleteSql(String table, List<String> primaryKeys, List<KeyRow> keys)
      throws DataException {
    List<String> columnPredicates = new ArrayList<>();
    for (String primaryKey : primaryKeys) {
      columnPredicates.add(quoteIdentifier(primaryKey) + " = ?");
    }

    String rowPredicate = "(" + String.join(" AND ", columnPredicates) + ")";
    List<String> predicates = Collections.nCopies(keys.size(), rowPredicate);
    return String.format("DELETE FROM %s WHERE (%s)", quoteIdentifier(table), String.join(" OR ", predicates));
  }

  private static void validateDeleteBatch(List<String> primaryKeys, List<KeyRow> keys) throws DataException {
    if (primaryKeys.isEmpty()) {
      throw new DataException("Primary key metadata cannot be empty");
    }
    int bindParameterCount = primaryKeys.size() * keys.size();
    if (bindParameterCount > ParameterConstants.MAX_DELETE_BIND_PARAMETERS) {
      throw new DataException(String.format(
          "Delete batch requires %d bind parameters, maximum is %d. Reduce --limit.",
          bindParameterCount, ParameterConstants.MAX_DELETE_BIND_PARAMETERS));
    }
    for (KeyRow key : keys) {
      validateKeyRow(primaryKeys, key);
    }
  }

  private static void validateKeyRow(List<String> primaryKeys, KeyRow key) throws DataException {
    if (!key.getColumns().equals(primaryKeys)) {
      throw new DataException("Fetched key row does not match primary key metadata");
    }
  }

  private static Optional<TableMetadata> readTableInfo(DatabaseMetaData meta, String catalogName,
      String schemaName, String tableName, String[] tableTypes)
      throws SQLException {
    try (ResultSet resultSet = meta.getTables(catalogName, schemaName, tableName, tableTypes)) {
      if (resultSet.next()) {
        return Optional.of(new TableMetadata(
            resultSet.getString("TABLE_NAME"),
            resultSet.getString("TABLE_SCHEM"),
            resultSet.getString("TABLE_CAT"),
            resultSet.getString("TABLE_TYPE")));
      }
    }
    return Optional.empty();
  }

  private static List<String> candidates(String value) {
    if (value == null || value.isEmpty()) {
      return Collections.singletonList(null);
    }
    Set<String> values = new LinkedHashSet<>();
    values.add(value);
    values.add(value.toUpperCase(Locale.ROOT));
    values.add(value.toLowerCase(Locale.ROOT));
    return new ArrayList<>(values);
  }
}
