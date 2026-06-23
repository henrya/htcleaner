package com.henrya.tools.htcleaner.dialects;

import com.henrya.tools.htcleaner.constants.ProcessorConstants;
import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.model.KeyRow;
import com.henrya.tools.htcleaner.tools.DataCreator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class H2DialectTest {

  private static final String TABLE_NAME = "H2_DIALECT_TEST";
  private Connection connection;
  private final H2Dialect dialect = new H2Dialect();

  @BeforeEach
  void setUp() throws SQLException {
    connection = DriverManager.getConnection(ProcessorConstants.CONN_URI_H2, "sa", "");
    DataCreator.createData(TABLE_NAME, 10);
  }

  @AfterEach
  void tearDown() throws SQLException {
    DataCreator.executeUpdate("DROP TABLE IF EXISTS " + TABLE_NAME);
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  @Test
  @DisplayName("Test get connection URI")
  void testGetConnectionURI() {
    assertThat(dialect.getConnectionURI()).isEqualTo(ProcessorConstants.CONN_URI_H2);
  }

  @Test
  @DisplayName("Test get primary keys")
  void testGetPrimaryKeys() throws DataException {
    List<String> primaryKeys = dialect.getPrimaryKeys(connection, TABLE_NAME);
    assertThat(primaryKeys).hasSize(1).contains("ID");
  }

  @Test
  @DisplayName("Test get primary keys from a table that does not exist")
  void testGetPrimaryKeysFromNonExistentTable() {
    assertThatThrownBy(() -> dialect.getPrimaryKeys(connection, "NON_EXISTENT_TABLE"))
        .isInstanceOf(DataException.class)
        .hasMessage("Table not found: NON_EXISTENT_TABLE");
  }

  @Test
  @DisplayName("Test get records")
  void testGetRecords() throws DataException {
    List<KeyRow> records = dialect.getRecords(connection, TABLE_NAME, Collections.singletonList("ID"), null, 5);
    assertThat(records).hasSize(5);
    assertThat(records).extracting(record -> String.valueOf(record.getValues().get(0))).contains("1", "2", "3", "4", "5");
  }

  @Test
  @DisplayName("Test get records with where clause")
  void testGetRecordsWithWhere() throws DataException {
    List<KeyRow> records = dialect.getRecords(connection, TABLE_NAME, Collections.singletonList("ID"), "ID > 5", 5);
    assertThat(records).hasSize(5);
    assertThat(records).extracting(record -> String.valueOf(record.getValues().get(0))).contains("6", "7", "8", "9", "10");
  }

  @Test
  @DisplayName("Test get records from a table that does not exist")
  void testGetRecordsFromNonExistentTable() {
    assertThatThrownBy(() -> dialect.getRecords(connection, "NON_EXISTENT_TABLE", Collections.singletonList("ID"), null, 5))
        .isInstanceOf(DataException.class)
        .hasMessageStartingWith("Cannot get records, table NON_EXISTENT_TABLE:");
  }
}
