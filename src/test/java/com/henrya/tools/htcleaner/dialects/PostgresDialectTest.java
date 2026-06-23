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
import java.util.Arrays;
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

class PostgresDialectTest {

  private final PostgresDialect dialect = new PostgresDialect();

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
    when(connection.getSchema()).thenReturn("public");
    when(connection.createStatement()).thenReturn(statement);
  }

  @Test
  @DisplayName("Test get connection URI")
  void testGetConnectionURI() {
    assertThat(dialect.getConnectionURI()).isEqualTo(ProcessorConstants.CONN_URI_POSTGRES);
  }

  @Test
  @DisplayName("Test get primary keys")
  void testGetPrimaryKeys() throws SQLException, DataException {
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getString(1)).thenReturn("id");

    List<String> primaryKeys = dialect.getPrimaryKeys(connection, "a_table");
    assertThat(primaryKeys).hasSize(1).contains("id");
    verify(preparedStatement).setString(1, "public");
    verify(preparedStatement).setString(2, "a_table");
  }

  @Test
  @DisplayName("Primary key lookup uses explicit PostgreSQL schema and table names")
  void testGetPrimaryKeysWithQualifiedMixedCaseName() throws SQLException, DataException {
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getString(1)).thenReturn("OrderId");

    List<String> primaryKeys = dialect.getPrimaryKeys(connection, "sales.Orders");

    assertThat(primaryKeys).containsExactly("OrderId");
    verify(connection).prepareStatement(org.mockito.ArgumentMatchers.argThat(sql ->
        sql.contains("n.nspname = ?")
            && sql.contains("c.relname = ?")
            && !sql.contains("::regclass")));
    verify(preparedStatement).setString(1, "sales");
    verify(preparedStatement).setString(2, "Orders");
  }

  @Test
  @DisplayName("Test get primary keys with SQL exception")
  void testGetPrimaryKeysWithSqlException() throws SQLException {
    when(preparedStatement.executeQuery()).thenThrow(new SQLException("mock exception"));
    assertThatThrownBy(() -> dialect.getPrimaryKeys(connection, "a_table"))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("Cannot find primary keys");
  }

  @Test
  @DisplayName("Test get records")
  void testGetRecords() throws SQLException, DataException {
    String sql = "SELECT \"id\" FROM \"a_table\"  ORDER BY \"id\" LIMIT 10";
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
    String sql = "SELECT \"id\" FROM \"a_table\" WHERE a_column > 1 ORDER BY \"id\" LIMIT 10";
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
    String sql = "SELECT \"id\" FROM \"a_table\"  ORDER BY \"id\" LIMIT 10";
    when(statement.executeQuery(sql)).thenThrow(new SQLException("mock exception"));

    assertThatThrownBy(() -> dialect.getRecords(connection, "a_table", Collections.singletonList("id"), null, 10))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("Cannot get records");
  }

  @Test
  @DisplayName("Unqualified table metadata lookup is restricted to current schema and includes partitioned tables")
  void testGetTableUsesCurrentSchemaAndPartitionedTableType() throws SQLException, DataException {
    when(databaseMetaData.getTables(eq(null), eq("public"), eq("orders"), any(String[].class)))
        .thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString("TABLE_NAME")).thenReturn("orders");
    when(resultSet.getString("TABLE_SCHEM")).thenReturn("public");
    when(resultSet.getString("TABLE_CAT")).thenReturn(null);
    when(resultSet.getString("TABLE_TYPE")).thenReturn("PARTITIONED TABLE");

    Optional<com.henrya.tools.htcleaner.model.TableMetadata> tableInfo = dialect.getTable(connection, "orders");

    assertThat(tableInfo).isPresent();
    assertThat(tableInfo.get().qualifiedName()).isEqualTo("public.orders");
    org.mockito.ArgumentCaptor<String[]> types = org.mockito.ArgumentCaptor.forClass(String[].class);
    verify(databaseMetaData).getTables(isNull(), eq("public"), eq("orders"), types.capture());
    assertThat(Arrays.asList(types.getValue())).containsExactly("TABLE", "PARTITIONED TABLE");
  }
}
