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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MySqlDialectTest {

  private final MySqlDialect dialect = new MySqlDialect();

  @Mock
  private Connection connection;

  @Mock
  private Statement statement;

  @Mock
  private ResultSet resultSet;

  @Mock
  private DatabaseMetaData databaseMetaData;

  @BeforeEach
  void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    when(connection.createStatement()).thenReturn(statement);
    when(connection.getMetaData()).thenReturn(databaseMetaData);
    when(connection.getCatalog()).thenReturn("app_db");
  }

  @Test
  @DisplayName("Test get connection URI")
  void testGetConnectionURI() {
    assertThat(dialect.getConnectionURI()).isEqualTo(ProcessorConstants.CONN_URI_MYSQL);
  }

  @Test
  @DisplayName("Test get primary keys")
  void testGetPrimaryKeys() throws SQLException, DataException {
    when(statement.executeQuery("SHOW COLUMNS FROM `a_table`")).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getString("KEY")).thenReturn("PRI");
    when(resultSet.getString("FIELD")).thenReturn("id");

    List<String> primaryKeys = dialect.getPrimaryKeys(connection, "a_table");
    assertThat(primaryKeys).hasSize(1).contains("id");
  }

  @Test
  @DisplayName("Test get primary keys with SQL exception")
  void testGetPrimaryKeysWithSqlException() throws SQLException {
    when(statement.executeQuery("SHOW COLUMNS FROM `a_table`")).thenThrow(new SQLException("mock exception"));
    assertThatThrownBy(() -> dialect.getPrimaryKeys(connection, "a_table"))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("Cannot find primary keys");
  }

  @Test
  @DisplayName("Test get records")
  void testGetRecords() throws SQLException, DataException {
    String sql = "SELECT `id` FROM `a_table`  ORDER BY `id` LIMIT 10";
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
    String sql = "SELECT `id` FROM `a_table`  WHERE a_column > 1 ORDER BY `id` LIMIT 10";
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
    String sql = "SELECT `id` FROM `a_table`  ORDER BY `id` LIMIT 10";
    when(statement.executeQuery(sql)).thenThrow(new SQLException("mock exception"));

    assertThatThrownBy(() -> dialect.getRecords(connection, "a_table", Collections.singletonList("id"), null, 10))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("Cannot get records");
  }

  @Test
  @DisplayName("Unqualified table metadata lookup is restricted to current database")
  void testGetTableUsesCurrentCatalog() throws SQLException, DataException {
    when(databaseMetaData.getTables(eq("app_db"), isNull(), eq("a_table"), any(String[].class)))
        .thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString("TABLE_NAME")).thenReturn("a_table");
    when(resultSet.getString("TABLE_SCHEM")).thenReturn(null);
    when(resultSet.getString("TABLE_CAT")).thenReturn("app_db");
    when(resultSet.getString("TABLE_TYPE")).thenReturn("TABLE");

    Optional<com.henrya.tools.htcleaner.model.TableMetadata> tableInfo = dialect.getTable(connection, "a_table");

    assertThat(tableInfo).isPresent();
    assertThat(tableInfo.get().qualifiedName()).isEqualTo("app_db.a_table");
    verify(databaseMetaData).getTables(eq("app_db"), isNull(), eq("a_table"), any(String[].class));
  }

  @Test
  @DisplayName("Unqualified table metadata lookup rejects missing current database")
  void testGetTableRejectsMissingCurrentCatalog() throws SQLException {
    when(connection.getCatalog()).thenReturn(null);

    assertThatThrownBy(() -> dialect.getTable(connection, "a_table"))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("Cannot resolve current database");
  }
}
