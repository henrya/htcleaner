package com.henrya.tools.htcleaner.dialects;

import com.henrya.tools.htcleaner.constants.ProcessorConstants;
import com.henrya.tools.htcleaner.exception.DataException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class PostgresDialectTest {

  private final PostgresDialect dialect = new PostgresDialect();

  @Mock
  private Connection connection;

  @Mock
  private PreparedStatement preparedStatement;

  @Mock
  private ResultSet resultSet;

  @BeforeEach
  void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
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
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getString("id")).thenReturn("1", "2");

    List<String> records = dialect.getRecords(connection, "a_table", "id", null, 10);
    assertThat(records).hasSize(2).contains("1", "2");
  }

  @Test
  @DisplayName("Test get records with where clause")
  void testGetRecordsWithWhere() throws SQLException, DataException {
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getString("id")).thenReturn("1", "2");

    List<String> records = dialect.getRecords(connection, "a_table", "id", "a_column > 1", 10);
    assertThat(records).hasSize(2).contains("1", "2");
  }

  @Test
  @DisplayName("Test get records with SQL exception")
  void testGetRecordsWithSqlException() throws SQLException {
    when(preparedStatement.executeQuery()).thenThrow(new SQLException("mock exception"));

    assertThatThrownBy(() -> dialect.getRecords(connection, "a_table", "id", null, 10))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("Cannot get records");
  }
}
