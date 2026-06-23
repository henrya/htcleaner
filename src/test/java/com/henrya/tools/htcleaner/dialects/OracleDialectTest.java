package com.henrya.tools.htcleaner.dialects;

import com.henrya.tools.htcleaner.constants.ProcessorConstants;
import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.model.KeyRow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OracleDialectTest {

  private final OracleDialect dialect = new OracleDialect();

  @Mock
  private Connection connection;

  @Mock
  private PreparedStatement preparedStatement;

  @Mock
  private ResultSet resultSet;

  @Mock
  private DatabaseMetaData databaseMetaData;

  @Mock
  private Statement statement;

  @BeforeEach
  void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    when(connection.getMetaData()).thenReturn(databaseMetaData);
    when(connection.getSchema()).thenReturn("APP");
    when(connection.createStatement()).thenReturn(statement);
  }

  @Test
  @DisplayName("Test get connection URI")
  void testGetConnectionURI() {
    assertThat(dialect.getConnectionURI()).isEqualTo(ProcessorConstants.CONN_URI_ORACLE);
  }

  @Test
  @DisplayName("Test get primary keys")
  void testGetPrimaryKeys() throws SQLException, DataException {
    when(databaseMetaData.getPrimaryKeys(null, "APP", "A_TABLE")).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getShort("KEY_SEQ")).thenReturn((short) 2, (short) 1);
    when(resultSet.getString("COLUMN_NAME")).thenReturn("id", "tenant_id");

    List<String> primaryKeys = dialect.getPrimaryKeys(connection, "a_table");
    assertThat(primaryKeys).containsExactly("tenant_id", "id");
    verify(databaseMetaData).getPrimaryKeys(null, "APP", "A_TABLE");
  }

  @Test
  @DisplayName("Test get primary keys with SQL exception")
  void testGetPrimaryKeysWithSqlException() throws SQLException {
    when(databaseMetaData.getPrimaryKeys(null, "APP", "A_TABLE")).thenThrow(new SQLException("mock exception"));
    assertThatThrownBy(() -> dialect.getPrimaryKeys(connection, "a_table"))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("Cannot find primary keys");
  }

  @Test
  @DisplayName("Test get records")
  void testGetRecords() throws SQLException, DataException {
    String sql = "SELECT \"ID\" FROM (SELECT \"ID\" FROM \"A_TABLE\" ORDER BY \"ID\") WHERE ROWNUM <= 10";
    when(statement.executeQuery(sql)).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getObject("id")).thenReturn("1", "2");

    List<KeyRow> records = dialect.getRecords(connection, "a_table", Collections.singletonList("id"), null, 10);
    assertThat(records).hasSize(2);
    assertThat(records).extracting(record -> record.getValues().get(0)).contains("1", "2");
  }

  @Test
  @DisplayName("Test get records with where clause")
  void testGetRecordsWithWhere() throws SQLException, DataException {
    String sql = "SELECT \"ID\" FROM (SELECT \"ID\" FROM \"A_TABLE\" WHERE a_column > 1 ORDER BY \"ID\") "
        + "WHERE ROWNUM <= 10";
    when(statement.executeQuery(sql)).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getObject("id")).thenReturn("1", "2");

    List<KeyRow> records = dialect.getRecords(connection, "a_table", Collections.singletonList("id"),
        "a_column > 1", 10);
    assertThat(records).hasSize(2);
    assertThat(records).extracting(record -> record.getValues().get(0)).contains("1", "2");
  }

  @Test
  @DisplayName("Test get records with SQL exception")
  void testGetRecordsWithSqlException() throws SQLException {
    String sql = "SELECT \"ID\" FROM (SELECT \"ID\" FROM \"A_TABLE\" ORDER BY \"ID\") WHERE ROWNUM <= 10";
    when(statement.executeQuery(sql)).thenThrow(new SQLException("mock exception"));

    assertThatThrownBy(() -> dialect.getRecords(connection, "a_table", Collections.singletonList("id"), null, 10))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("Cannot get records");
  }

  @Test
  @DisplayName("Unqualified table metadata lookup is restricted to current schema")
  void testGetTableUsesCurrentSchema() throws SQLException, DataException {
    when(databaseMetaData.getTables(isNull(), eq("APP"), eq("A_TABLE"), any(String[].class)))
        .thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString("TABLE_NAME")).thenReturn("A_TABLE");
    when(resultSet.getString("TABLE_SCHEM")).thenReturn("APP");
    when(resultSet.getString("TABLE_CAT")).thenReturn(null);
    when(resultSet.getString("TABLE_TYPE")).thenReturn("TABLE");

    Optional<com.henrya.tools.htcleaner.model.TableMetadata> tableInfo = dialect.getTable(connection, "a_table");

    assertThat(tableInfo).isPresent();
    assertThat(tableInfo.get().qualifiedName()).isEqualTo("APP.A_TABLE");
    verify(databaseMetaData).getTables(isNull(), eq("APP"), eq("A_TABLE"), any(String[].class));
  }

  @Test
  @DisplayName("Oracle delete rejects oversized single-column IN lists")
  void testBuildDeleteSqlRejectsOversizedSingleColumnInList() {
    List<KeyRow> keys = new ArrayList<>();
    List<String> primaryKeys = Collections.singletonList("id");
    for (int index = 0; index < 1_001; index++) {
      keys.add(new KeyRow(primaryKeys, Collections.singletonList(index)));
    }

    assertThatThrownBy(() -> dialect.buildDeleteSql("a_table", primaryKeys, null, keys))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("maximum is 1000");
  }
}
