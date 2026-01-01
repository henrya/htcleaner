package com.henrya.tools.htcleaner.driver;

import com.henrya.tools.htcleaner.Cleaner;
import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.tools.DataCreator;
import com.henrya.tools.htcleaner.tools.TestConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CleanerDriverImplTest {

  private CleanerDriverImpl cleanerDriver;

  @BeforeEach
  void init() throws Exception{
    cleanerDriver = new CleanerDriverImpl("h2");
    Cleaner cleaner = TestConfig.getCleaner();
    cleanerDriver.connect(cleaner.getHost(), cleaner.getPort(), cleaner.getDatabase(),
        cleaner.getUser(), cleaner.getPassword());
  }

  @AfterEach
  void cleanTable() {
    DataCreator.executeUpdate("DROP TABLE IF EXISTS "+TestConfig.TABLE_NAME);
  }

  @Test
  @DisplayName("Connection test")
  void testConnection(){
    Cleaner cleaner = TestConfig.getCleaner();
    try {
      cleanerDriver.connect(cleaner.getHost(), cleaner.getPort(), cleaner.getDatabase(),
          cleaner.getUser(), cleaner.getPassword());
      Connection connection = cleanerDriver.getConn();
      assertThat(connection).isNotNull();
    }catch (DataException e){
      Assertions.fail();
    }
    assertThat(cleanerDriver).isNotNull();
  }

  @Test
  @DisplayName("Connection test failure")
  void testConnectionFailure(){
    Cleaner cleaner = TestConfig.getCleaner();

      assertThatThrownBy(() -> cleanerDriver.connect(cleaner.getHost(), cleaner.getPort(), cleaner.getDatabase(),
          cleaner.getUser(), "abc")).isInstanceOf(
              DataException.class)
          .hasMessageContaining("Failed to connect: Wrong user name or password [28000-240]");

    assertThat(cleanerDriver).isNotNull();
  }

  @Test
  @DisplayName("Connection test failure, mysql driver")
  void testConnectionFailureMysqlDriver(){
    CleanerDriverImpl mySqlCleanerDriver = new CleanerDriverImpl("mysql");
    Cleaner cleaner = TestConfig.getCleaner();

    assertThatThrownBy(() -> mySqlCleanerDriver.connect(cleaner.getHost(), cleaner.getPort(), cleaner.getDatabase(),
        cleaner.getUser(), "abc")).isInstanceOf(
            DataException.class)
        .hasMessageContaining( "Failed to connect: Communications link failure\n" + "\n"
            + "The last packet sent successfully to the server was 0 milliseconds ago. The driver has not received any packets from the server.");

    assertThat(mySqlCleanerDriver).isNotNull();
  }

  @Test
  @DisplayName("Connection test failure, unknown driver")
  void testConnectionFailureRandomDriver(){
    CleanerDriverImpl randomCleanerDriver = new CleanerDriverImpl("random");
    Cleaner cleaner = TestConfig.getCleaner();

    assertThatThrownBy(() -> randomCleanerDriver.connect(cleaner.getHost(), cleaner.getPort(), cleaner.getDatabase(),
        cleaner.getUser(), "abc")).isInstanceOf(
            DataException.class)
        .hasMessageContaining("Failed to connect: Unexpected driver: random");

    assertThat(randomCleanerDriver).isNotNull();
  }

  @Test
  @DisplayName("Get primary keys")
  void testGetPrimaryKeys() {
    DataCreator.createData(TestConfig.TABLE_NAME,10);
    try {
      List<String> keys = cleanerDriver.getPrimaryKeys(TestConfig.TABLE_NAME);
      assertThat(keys).hasSize(1);
    } catch (DataException e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("Get primary keys, no primary keys")
  void testGetPrimaryKeysNoKeys() {
    try {
      DataCreator.createDataWithoutPK(TestConfig.TABLE_NAME,10);
      List<String> keys = cleanerDriver.getPrimaryKeys(TestConfig.TABLE_NAME);
      assertThat(keys).isEmpty();
    } catch (DataException e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("Get primary keys, no table")
  void testGetPrimaryKeysBrokenTable() {
    assertThatThrownBy(() -> cleanerDriver.getPrimaryKeys(";:*;;/")).isInstanceOf(DataException.class);
  }

  @Test
  @DisplayName("Get records by primary key")
  void testGetRecordsByPrimaryKey() {
    try {
      DataCreator.createData(TestConfig.TABLE_NAME,10);
      List<String> ids = cleanerDriver.getRecords(TestConfig.TABLE_NAME,"id", null, 10);
      assertThat(ids).hasSize(10);
    } catch (DataException e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("Get records by primary key and with where")
  void testGetRecordsByPrimaryKeyWithWhere() {
    try {
      DataCreator.createData(TestConfig.TABLE_NAME,10);
      List<String> ids = cleanerDriver.getRecords(TestConfig.TABLE_NAME,"id", "a LIKE 'a%'", 10);
      assertThat(ids).hasSize(10);
    } catch (DataException e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("Get records by primary key , exception")
  void testGetRecordsByPrimaryKeyException() {
    assertThatThrownBy(() -> cleanerDriver.getRecords(TestConfig.TABLE_NAME,"id", "a LIKE ''a%'", 1)).isInstanceOf(DataException.class);
  }

  @Test
  @DisplayName("Get records by primary key")
  void tesDeleteRecordsByPrimaryKey() {
    try {
      DataCreator.createData(TestConfig.TABLE_NAME,10);
      List<String> keys = new ArrayList<>();
      for(int i=1;i<=10;i++){
        keys.add(String.valueOf(i));
      }
      int removed = cleanerDriver.deleteRecords(TestConfig.TABLE_NAME,"id", null, keys, true);
      assertThat(removed).isEqualTo(10);
    } catch (DataException e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("Get records by primary key and with where")
  void tesDeleteRecordsByPrimaryKeyWithWhere() {
    try {
      DataCreator.createData(TestConfig.TABLE_NAME,10);
      List<String> keys = new ArrayList<>();
      for(int i=1;i<=10;i++){
        keys.add(String.valueOf(i));
      }
      int removed = cleanerDriver.deleteRecords(TestConfig.TABLE_NAME,"id", "a LIKE 'a%'", keys, true);
      assertThat(removed).isEqualTo(10);
    } catch (DataException e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("Delete records by primary key, do not commit")
  void testDeleteRecordsByPrimaryKeyDoNotCommit() {
    try {
      DataCreator.createData(TestConfig.TABLE_NAME+"a",10);
      List<String> keys = new ArrayList<>();
      for(int i=1;i<=10;i++){
        keys.add(String.valueOf(i));
      }
      int removed = cleanerDriver.deleteRecords(TestConfig.TABLE_NAME+"a","id", null, keys, false);
      assertThat(removed).isEqualTo(10);

      int amount = DataCreator.executeCount(TestConfig.TABLE_NAME+"a", null);
      assertThat(amount).isEqualTo(10);
    } catch (DataException e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("Delete records by primary key , exception")
  void testDeleteRecordsByPrimaryKeyException() {
    assertThatThrownBy(() -> cleanerDriver.deleteRecords(TestConfig.TABLE_NAME,"id", "a LIKE ''a%'", new ArrayList<>(), true)).isInstanceOf(DataException.class);
  }

  @Test
  @DisplayName("Delete records by primary key , exception on commit")
  void testDeleteRecordsByPrimaryKeyExceptionOnCommit() throws SQLException {
    Connection connection = Mockito.mock(Connection.class);
    cleanerDriver.setConn(connection);
    Mockito.doNothing().when(connection).setAutoCommit(Mockito.anyBoolean());
    Mockito.doThrow(SQLException.class).when(connection).setAutoCommit(Mockito.anyBoolean());

    DataCreator.createData(TestConfig.TABLE_NAME + "b", 10);
    List<String> keys = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      keys.add(String.valueOf(i));
    }

    assertThatThrownBy(() -> cleanerDriver.deleteRecords(TestConfig.TABLE_NAME + "b", "id", null, keys, false)).isInstanceOf(DataException.class);

    int amount = DataCreator.executeCount(TestConfig.TABLE_NAME + "b", null);
    assertThat(amount).isEqualTo(10);
  }

  @Test
  @DisplayName("Get table schema")
  void testGetTableSchema() {
    try {
      DataCreator.createData(TestConfig.TABLE_NAME,1);
      Map<String, String> tableInfo = cleanerDriver.getTable(TestConfig.TABLE_NAME);
      assertThat(tableInfo).hasSize(4);
      assertThat(tableInfo).containsKeys("name","schema","catalog","type");
    } catch (DataException e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("Get table meta data , exception")
  void testGetTableSchemaMockException() throws SQLException{
    Connection connection = Mockito.mock(Connection.class);
    cleanerDriver.setConn(connection);
    Mockito.when(connection.getMetaData()).thenThrow(SQLException.class);
    assertThatThrownBy(() -> cleanerDriver.getTable(TestConfig.TABLE_NAME)).isInstanceOf(DataException.class);
    Mockito.verify(connection,Mockito.times(1)).getMetaData();
  }

  @Test
  @DisplayName("Get records by primary key")
  void testCountRecords() {
    try {
      DataCreator.createData(TestConfig.TABLE_NAME,10);
      int amount = cleanerDriver.countRows(TestConfig.TABLE_NAME, null);
      assertThat(amount).isEqualTo(10);
    } catch (DataException e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("Get records by primary key and with where")
  void testCountRecordsWithWhere() {
    try {
      DataCreator.createData(TestConfig.TABLE_NAME,10);
      int amount = cleanerDriver.countRows(TestConfig.TABLE_NAME, "a LIKE 'a%'");
      assertThat(amount).isEqualTo(10);
    } catch (DataException e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("Get records by primary key , exception")
  void testCountRecordsByPrimaryKeyException() {
    assertThatThrownBy(() -> cleanerDriver.countRows(TestConfig.TABLE_NAME, "a LIKE ''a%'")).isInstanceOf(DataException.class);
  }
}
