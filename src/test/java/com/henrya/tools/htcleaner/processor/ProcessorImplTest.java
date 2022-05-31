package com.henrya.tools.htcleaner.processor;

import com.henrya.tools.htcleaner.Cleaner;
import com.henrya.tools.htcleaner.driver.CleanerDriverImpl;
import com.henrya.tools.htcleaner.exception.CleanerException;
import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.tools.TestConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProcessorImplTest {

  @Test
  @DisplayName("General processor execution")
  void testProcessor() throws Exception {
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Connection connection = Mockito.mock(Connection.class);

    //cleanerDriver.setConn("abc");
    Mockito.doNothing().when(cleanerDriver).connect(Mockito.anyString(),Mockito.anyInt(),Mockito.anyString(),Mockito.anyString(),Mockito.anyString());
    Mockito.when(cleanerDriver.getConn()).thenReturn(connection);

    Map<String, String> tableInfo = new HashMap<>();
    tableInfo.put("name", cleaner.getTable());
    List<String> primaryKeys = new ArrayList<>();
    primaryKeys.add("ID");

    Mockito.when(cleanerDriver.getTable(Mockito.anyString())).thenReturn(tableInfo);
    Mockito.when(cleanerDriver.getPrimaryKeys(Mockito.anyString())).thenReturn(primaryKeys);
    Mockito.when(cleanerDriver.countRows(Mockito.anyString(),Mockito.any())).thenReturn(2);
    // executor
    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        Arrays.asList("1", "2")).thenReturn(new ArrayList<>());
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn(2);

    try {
      ProcessorImpl processor = new ProcessorImpl();
      processor.process(cleaner,cleanerDriver);

      // driver
      Mockito.verify(cleanerDriver,Mockito.times(1)).getConn();
      Mockito.verify(cleanerDriver,Mockito.times(1)).getTable(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.times(1)).getPrimaryKeys(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.times(1)).countRows(Mockito.anyString(), Mockito.any());
      Mockito.verify(cleanerDriver,Mockito.times(2)).getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt());
      Mockito.verify(cleanerDriver,Mockito.times(1)).deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean());

      // cleaner
      Mockito.verify(cleaner,Mockito.times(1)).getHost();
      Mockito.verify(cleaner,Mockito.times(1)).getDatabase();
      Mockito.verify(cleaner,Mockito.times(1)).getUser();
      Mockito.verify(cleaner,Mockito.times(1)).getPassword();
      Mockito.verify(cleaner,Mockito.times(8)).getTable();
      Mockito.verify(cleaner,Mockito.times(4)).getPrimaryKey();
      Mockito.verify(cleaner,Mockito.times(1)).isNotQuiet();
      Mockito.verify(cleaner,Mockito.times(1)).getProgressDelay();


    } catch (Exception e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("General processor execution with custom PK that exists")
  void testProcessorCustomPK() throws Exception {
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());
    cleaner.setPrimaryKey("ID");
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Connection connection = Mockito.mock(Connection.class);

    Mockito.doNothing().when(cleanerDriver).connect(Mockito.anyString(),Mockito.anyInt(),Mockito.anyString(),Mockito.anyString(),Mockito.anyString());
    Mockito.when(cleanerDriver.getConn()).thenReturn(connection);

    Map<String, String> tableInfo = new HashMap<>();
    tableInfo.put("name", cleaner.getTable());
    List<String> primaryKeys = new ArrayList<>();
    primaryKeys.add("ID");

    Mockito.when(cleanerDriver.getTable(Mockito.anyString())).thenReturn(tableInfo);
    Mockito.when(cleanerDriver.getPrimaryKeys(Mockito.anyString())).thenReturn(primaryKeys);
    Mockito.when(cleanerDriver.countRows(Mockito.anyString(),Mockito.any())).thenReturn(2);
    // executor
    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        Arrays.asList("1", "2")).thenReturn(new ArrayList<>());
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn(2);

    try {
      ProcessorImpl processor = new ProcessorImpl();
      processor.process(cleaner,cleanerDriver);

      // driver
      Mockito.verify(cleanerDriver,Mockito.times(1)).getConn();
      Mockito.verify(cleanerDriver,Mockito.times(1)).getTable(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.times(1)).getPrimaryKeys(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.times(1)).countRows(Mockito.anyString(), Mockito.any());
      Mockito.verify(cleanerDriver,Mockito.times(2)).getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt());
      Mockito.verify(cleanerDriver,Mockito.times(1)).deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean());

      // cleaner
      Mockito.verify(cleaner,Mockito.times(1)).getHost();
      Mockito.verify(cleaner,Mockito.times(1)).getDatabase();
      Mockito.verify(cleaner,Mockito.times(1)).getUser();
      Mockito.verify(cleaner,Mockito.times(1)).getPassword();
      Mockito.verify(cleaner,Mockito.times(8)).getTable();
      Mockito.verify(cleaner,Mockito.times(7)).getPrimaryKey();
      Mockito.verify(cleaner,Mockito.times(1)).isNotQuiet();
      Mockito.verify(cleaner,Mockito.times(1)).getProgressDelay();


    } catch (Exception e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("General processor execution with custom PK that does not exist")
  void testProcessorCustomPKNotExists() throws Exception {
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());
    cleaner.setPrimaryKey("PK");
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Connection connection = Mockito.mock(Connection.class);

    Mockito.doNothing().when(cleanerDriver).connect(Mockito.anyString(),Mockito.anyInt(),Mockito.anyString(),Mockito.anyString(),Mockito.anyString());
    Mockito.when(cleanerDriver.getConn()).thenReturn(connection);

    Map<String, String> tableInfo = new HashMap<>();
    tableInfo.put("name", cleaner.getTable());
    List<String> primaryKeys = new ArrayList<>();
    primaryKeys.add("ID");

    Mockito.when(cleanerDriver.getTable(Mockito.anyString())).thenReturn(tableInfo);
    Mockito.when(cleanerDriver.getPrimaryKeys(Mockito.anyString())).thenReturn(primaryKeys);
    Mockito.when(cleanerDriver.countRows(Mockito.anyString(),Mockito.any())).thenReturn(2);
    // executor
    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        Arrays.asList("1", "2")).thenReturn(new ArrayList<>());
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn(2);

    try {
      ProcessorImpl processor = new ProcessorImpl();

      assertThatThrownBy(() -> processor.process(cleaner,cleanerDriver)).isInstanceOf(CleanerException.class)
          .hasMessageContaining("Primary key: PK is not valid");

      // driver
      Mockito.verify(cleanerDriver,Mockito.times(1)).getConn();
      Mockito.verify(cleanerDriver,Mockito.times(1)).getTable(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.times(1)).getPrimaryKeys(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.never()).countRows(Mockito.anyString(), Mockito.any());
      Mockito.verify(cleanerDriver,Mockito.never()).getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt());
      Mockito.verify(cleanerDriver,Mockito.never()).deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean());

      // cleaner
      Mockito.verify(cleaner,Mockito.times(1)).getHost();
      Mockito.verify(cleaner,Mockito.times(1)).getDatabase();
      Mockito.verify(cleaner,Mockito.times(1)).getUser();
      Mockito.verify(cleaner,Mockito.times(1)).getPassword();
      Mockito.verify(cleaner,Mockito.times(3)).getTable();
      Mockito.verify(cleaner,Mockito.times(4)).getPrimaryKey();
      Mockito.verify(cleaner,Mockito.never()).isNotQuiet();
      Mockito.verify(cleaner,Mockito.never()).getProgressDelay();


    } catch (Exception e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("General processor execution, do not count rows")
  void testProcessorDoNotCountRows() throws Exception {
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Connection connection = Mockito.mock(Connection.class);

    // do not count rows
    cleaner.setCountRows(false);

    Mockito.doNothing().when(cleanerDriver).connect(Mockito.anyString(),Mockito.anyInt(),Mockito.anyString(),Mockito.anyString(),Mockito.anyString());
    Mockito.when(cleanerDriver.getConn()).thenReturn(connection);

    Map<String, String> tableInfo = new HashMap<>();
    tableInfo.put("name", cleaner.getTable());
    List<String> primaryKeys = new ArrayList<>();
    primaryKeys.add("ID");

    Mockito.when(cleanerDriver.getTable(Mockito.anyString())).thenReturn(tableInfo);
    Mockito.when(cleanerDriver.getPrimaryKeys(Mockito.anyString())).thenReturn(primaryKeys);
    Mockito.when(cleanerDriver.countRows(Mockito.anyString(),Mockito.any())).thenReturn(2);
    // executor
    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        Arrays.asList("1", "2")).thenReturn(new ArrayList<>());
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn(2);

    try {
      ProcessorImpl processor = new ProcessorImpl();
      processor.process(cleaner,cleanerDriver);

      // driver
      Mockito.verify(cleanerDriver,Mockito.times(1)).getConn();
      Mockito.verify(cleanerDriver,Mockito.times(1)).getTable(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.times(1)).getPrimaryKeys(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.never()).countRows(Mockito.anyString(), Mockito.any());
      Mockito.verify(cleanerDriver,Mockito.times(2)).getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt());
      Mockito.verify(cleanerDriver,Mockito.times(1)).deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean());

      // cleaner
      Mockito.verify(cleaner,Mockito.times(1)).getHost();
      Mockito.verify(cleaner,Mockito.times(1)).getDatabase();
      Mockito.verify(cleaner,Mockito.times(1)).getUser();
      Mockito.verify(cleaner,Mockito.times(1)).getPassword();
      Mockito.verify(cleaner,Mockito.times(7)).getTable();
      Mockito.verify(cleaner,Mockito.times(4)).getPrimaryKey();
      Mockito.verify(cleaner,Mockito.times(1)).isNotQuiet();
      Mockito.verify(cleaner,Mockito.times(1)).getProgressDelay();


    } catch (Exception e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("General processor execution, quiet  kde")
  void testProcessorQuietMode() throws Exception {
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Connection connection = Mockito.mock(Connection.class);

    // do not count rows
    cleaner.setQuiet(true);

    Mockito.doNothing().when(cleanerDriver).connect(Mockito.anyString(),Mockito.anyInt(),Mockito.anyString(),Mockito.anyString(),Mockito.anyString());
    Mockito.when(cleanerDriver.getConn()).thenReturn(connection);

    Map<String, String> tableInfo = new HashMap<>();
    tableInfo.put("name", cleaner.getTable());
    List<String> primaryKeys = new ArrayList<>();
    primaryKeys.add("ID");

    Mockito.when(cleanerDriver.getTable(Mockito.anyString())).thenReturn(tableInfo);
    Mockito.when(cleanerDriver.getPrimaryKeys(Mockito.anyString())).thenReturn(primaryKeys);
    Mockito.when(cleanerDriver.countRows(Mockito.anyString(),Mockito.any())).thenReturn(2);
    // executor
    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        Arrays.asList("1", "2")).thenReturn(new ArrayList<>());
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn(2);

    try {
      ProcessorImpl processor = new ProcessorImpl();
      processor.process(cleaner,cleanerDriver);

      // driver
      Mockito.verify(cleanerDriver,Mockito.times(1)).getConn();
      Mockito.verify(cleanerDriver,Mockito.times(1)).getTable(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.times(1)).getPrimaryKeys(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.times(1)).countRows(Mockito.anyString(), Mockito.any());
      Mockito.verify(cleanerDriver,Mockito.times(2)).getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt());
      Mockito.verify(cleanerDriver,Mockito.times(1)).deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean());

      // cleaner
      Mockito.verify(cleaner,Mockito.times(1)).getHost();
      Mockito.verify(cleaner,Mockito.times(1)).getDatabase();
      Mockito.verify(cleaner,Mockito.times(1)).getUser();
      Mockito.verify(cleaner,Mockito.times(1)).getPassword();
      Mockito.verify(cleaner,Mockito.times(8)).getTable();
      Mockito.verify(cleaner,Mockito.times(4)).getPrimaryKey();
      Mockito.verify(cleaner,Mockito.times(1)).isNotQuiet();
      Mockito.verify(cleaner,Mockito.never()).getProgressDelay();


    } catch (Exception e){
      Assertions.fail();
    }
  }


  @Test
  @DisplayName("General processor execution, but data exception happens")
  void testProcessorDataException() throws Exception {
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());
    cleaner.setPrimaryKey("ID");
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Connection connection = Mockito.mock(Connection.class);

    //cleanerDriver.setConn("abc");
    Mockito.doNothing().when(cleanerDriver).connect(Mockito.anyString(),Mockito.anyInt(),Mockito.anyString(),Mockito.anyString(),Mockito.anyString());
    Mockito.when(cleanerDriver.getConn()).thenReturn(connection);

    Map<String, String> tableInfo = new HashMap<>();
    tableInfo.put("name", cleaner.getTable());

    Mockito.when(cleanerDriver.getTable(Mockito.anyString())).thenReturn(tableInfo);
    Mockito.when(cleanerDriver.getPrimaryKeys(Mockito.anyString())).thenThrow(new DataException("Cannot find rows"));
    Mockito.when(cleanerDriver.countRows(Mockito.anyString(),Mockito.any())).thenReturn(2);
    // executor
    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        Arrays.asList("1", "2")).thenReturn(new ArrayList<>());
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn(2);

    try {
      ProcessorImpl processor = new ProcessorImpl();

      processor.process(cleaner,cleanerDriver);

      // driver
      Mockito.verify(cleanerDriver,Mockito.times(1)).getConn();
      Mockito.verify(cleanerDriver,Mockito.times(1)).getTable(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.times(1)).getPrimaryKeys(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.never()).countRows(Mockito.anyString(), Mockito.any());
      Mockito.verify(cleanerDriver,Mockito.never()).getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt());
      Mockito.verify(cleanerDriver,Mockito.never()).deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean());

      // cleaner
      Mockito.verify(cleaner,Mockito.times(1)).getHost();
      Mockito.verify(cleaner,Mockito.times(1)).getDatabase();
      Mockito.verify(cleaner,Mockito.times(1)).getUser();
      Mockito.verify(cleaner,Mockito.times(1)).getPassword();
      Mockito.verify(cleaner,Mockito.times(3)).getTable();
      Mockito.verify(cleaner,Mockito.never()).getPrimaryKey();
      Mockito.verify(cleaner,Mockito.never()).isNotQuiet();
      Mockito.verify(cleaner,Mockito.never()).getProgressDelay();


    } catch (Exception e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("General processor execution, no primary keys")
  void testProcessorNoPrimaryKeys() throws Exception {
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());

    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Connection connection = Mockito.mock(Connection.class);

    //cleanerDriver.setConn("abc");
    Mockito.doNothing().when(cleanerDriver).connect(Mockito.anyString(),Mockito.anyInt(),Mockito.anyString(),Mockito.anyString(),Mockito.anyString());
    Mockito.when(cleanerDriver.getConn()).thenReturn(connection);

    Map<String, String> tableInfo = new HashMap<>();
    tableInfo.put("name", cleaner.getTable());

    Mockito.when(cleanerDriver.getTable(Mockito.anyString())).thenReturn(tableInfo);
    Mockito.when(cleanerDriver.getPrimaryKeys(Mockito.anyString())).thenReturn(new ArrayList<>());
    Mockito.when(cleanerDriver.countRows(Mockito.anyString(),Mockito.any())).thenReturn(2);
    // executor
    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        Arrays.asList("1", "2")).thenReturn(new ArrayList<>());
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn(2);

    try {
      ProcessorImpl processor = new ProcessorImpl();

      assertThatThrownBy(() -> processor.process(cleaner,cleanerDriver)).isInstanceOf(CleanerException.class)
          .hasMessageContaining("Cannot find primary keys!");

      // driver
      Mockito.verify(cleanerDriver,Mockito.times(1)).getConn();
      Mockito.verify(cleanerDriver,Mockito.times(1)).getTable(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.times(1)).getPrimaryKeys(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.never()).countRows(Mockito.anyString(), Mockito.any());
      Mockito.verify(cleanerDriver,Mockito.never()).getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt());
      Mockito.verify(cleanerDriver,Mockito.never()).deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean());

      // cleaner
      Mockito.verify(cleaner,Mockito.times(1)).getHost();
      Mockito.verify(cleaner,Mockito.times(1)).getDatabase();
      Mockito.verify(cleaner,Mockito.times(1)).getUser();
      Mockito.verify(cleaner,Mockito.times(1)).getPassword();
      Mockito.verify(cleaner,Mockito.times(3)).getTable();
      Mockito.verify(cleaner,Mockito.times(1)).getPrimaryKey();
      Mockito.verify(cleaner,Mockito.never()).isNotQuiet();
      Mockito.verify(cleaner,Mockito.never()).getProgressDelay();

    } catch (Exception e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("General processor execution, primaryKey parameter empty")
  void testProcessorEmptyCleanerPrimaryKey() throws Exception {
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());

    cleaner.setPrimaryKey("");

    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Connection connection = Mockito.mock(Connection.class);

    Mockito.doNothing().when(cleanerDriver).connect(Mockito.anyString(),Mockito.anyInt(),Mockito.anyString(),Mockito.anyString(),Mockito.anyString());
    Mockito.when(cleanerDriver.getConn()).thenReturn(connection);

    Map<String, String> tableInfo = new HashMap<>();
    tableInfo.put("name", cleaner.getTable());

    Mockito.when(cleanerDriver.getTable(Mockito.anyString())).thenReturn(tableInfo);
    Mockito.when(cleanerDriver.getPrimaryKeys(Mockito.anyString())).thenReturn(new ArrayList<>());
    Mockito.when(cleanerDriver.countRows(Mockito.anyString(),Mockito.any())).thenReturn(2);
    // executor
    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        Arrays.asList("1", "2")).thenReturn(new ArrayList<>());
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn(2);

    try {
      ProcessorImpl processor = new ProcessorImpl();

      assertThatThrownBy(() -> processor.process(cleaner,cleanerDriver)).isInstanceOf(CleanerException.class)
          .hasMessageContaining("Cannot find primary keys!");

      // driver
      Mockito.verify(cleanerDriver,Mockito.times(1)).getConn();
      Mockito.verify(cleanerDriver,Mockito.times(1)).getTable(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.times(1)).getPrimaryKeys(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.never()).countRows(Mockito.anyString(), Mockito.any());
      Mockito.verify(cleanerDriver,Mockito.never()).getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt());
      Mockito.verify(cleanerDriver,Mockito.never()).deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean());

      // cleaner
      Mockito.verify(cleaner,Mockito.times(1)).getHost();
      Mockito.verify(cleaner,Mockito.times(1)).getDatabase();
      Mockito.verify(cleaner,Mockito.times(1)).getUser();
      Mockito.verify(cleaner,Mockito.times(1)).getPassword();
      Mockito.verify(cleaner,Mockito.times(3)).getTable();
      Mockito.verify(cleaner,Mockito.times(2)).getPrimaryKey();
      Mockito.verify(cleaner,Mockito.never()).isNotQuiet();
      Mockito.verify(cleaner,Mockito.never()).getProgressDelay();

    } catch (Exception e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("General processor execution with custom PK that does not exist")
  void testProcessorTableNotExist() throws Exception {
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());
    cleaner.setPrimaryKey("PK");
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Connection connection = Mockito.mock(Connection.class);

    //cleanerDriver.setConn("abc");
    Mockito.doNothing().when(cleanerDriver).connect(Mockito.anyString(),Mockito.anyInt(),Mockito.anyString(),Mockito.anyString(),Mockito.anyString());
    Mockito.when(cleanerDriver.getConn()).thenReturn(connection);

    List<String> primaryKeys = new ArrayList<>();
    primaryKeys.add("ID");

    Mockito.when(cleanerDriver.getTable(Mockito.anyString())).thenReturn(new HashMap<>());
    Mockito.when(cleanerDriver.getPrimaryKeys(Mockito.anyString())).thenReturn(primaryKeys);
    Mockito.when(cleanerDriver.countRows(Mockito.anyString(),Mockito.any())).thenReturn(2);
    // executor
    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        Arrays.asList("1", "2")).thenReturn(new ArrayList<>());
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn(2);

    try {
      ProcessorImpl processor = new ProcessorImpl();
      processor.process(cleaner,cleanerDriver);
      // driver
      Mockito.verify(cleanerDriver,Mockito.times(1)).getConn();
      Mockito.verify(cleanerDriver,Mockito.times(1)).getTable(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.never()).getPrimaryKeys(Mockito.anyString());
      Mockito.verify(cleanerDriver,Mockito.never()).countRows(Mockito.anyString(), Mockito.any());
      Mockito.verify(cleanerDriver,Mockito.never()).getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt());
      Mockito.verify(cleanerDriver,Mockito.never()).deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean());

      // cleaner
      Mockito.verify(cleaner,Mockito.times(1)).getHost();
      Mockito.verify(cleaner,Mockito.times(1)).getDatabase();
      Mockito.verify(cleaner,Mockito.times(1)).getUser();
      Mockito.verify(cleaner,Mockito.times(1)).getPassword();
      Mockito.verify(cleaner,Mockito.times(2)).getTable();
      Mockito.verify(cleaner,Mockito.never()).getPrimaryKey();
      Mockito.verify(cleaner,Mockito.never()).isNotQuiet();
      Mockito.verify(cleaner,Mockito.never()).getProgressDelay();


    } catch (Exception e){
      Assertions.fail();
    }
  }

  @Test
  void testProcessorNoConnection() throws Exception {
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Mockito.doNothing().when(cleanerDriver).connect(Mockito.anyString(),Mockito.anyInt(),Mockito.anyString(),Mockito.anyString(),Mockito.anyString());
    try {
      ProcessorImpl processor = new ProcessorImpl();
      processor.process(cleaner,cleanerDriver);
      Mockito.verify(cleanerDriver,Mockito.never()).getTable(Mockito.anyString());
    } catch (CleanerException e){
      Assertions.fail();
    }
  }
}
