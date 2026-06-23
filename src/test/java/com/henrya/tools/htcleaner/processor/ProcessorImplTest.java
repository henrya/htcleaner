package com.henrya.tools.htcleaner.processor;

import com.henrya.tools.htcleaner.Cleaner;
import com.henrya.tools.htcleaner.driver.CleanerDriverImpl;
import com.henrya.tools.htcleaner.exception.CleanerException;
import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.model.KeyRow;
import com.henrya.tools.htcleaner.model.TableMetadata;
import com.henrya.tools.htcleaner.tools.TestConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

class ProcessorImplTest {

  @Test
  @DisplayName("General processor execution")
  void testProcessor() throws Exception {
    Cleaner cleaner = TestConfig.getCleaner();
    CleanerDriverImpl cleanerDriver = connectedDriver(cleaner, TestConfig.primaryKeys());
    Mockito.when(cleanerDriver.countRows(cleaner.getTable(), cleaner.getWhere())).thenReturn(2);
    Mockito.when(cleanerDriver.getRecords(anyString(), any(), any(), anyInt()))
        .thenReturn(TestConfig.keyRows("1", "2"))
        .thenReturn(Collections.emptyList());
    Mockito.when(cleanerDriver.deleteRecords(anyString(), any(), any(), any(), anyBoolean())).thenReturn(2);

    new ProcessorImpl().process(cleaner, cleanerDriver);

    Mockito.verify(cleanerDriver).connect(cleaner.getHost(), cleaner.getPort(), cleaner.getDatabase(),
        cleaner.getUser(), cleaner.getPassword());
    Mockito.verify(cleanerDriver).countRows(cleaner.getTable(), cleaner.getWhere());
    Mockito.verify(cleanerDriver, Mockito.times(2))
        .getRecords(eq(cleaner.getTable()), eq(TestConfig.primaryKeys()), eq(cleaner.getWhere()),
            eq(cleaner.getLimit()));
    Mockito.verify(cleanerDriver)
        .deleteRecords(eq(cleaner.getTable()), eq(TestConfig.primaryKeys()), eq(cleaner.getWhere()), any(), eq(true));
    assertThat(cleaner.getPrimaryKey()).isNull();
  }

  @Test
  @DisplayName("General processor execution with custom PK that exists")
  void testProcessorCustomPK() throws Exception {
    Cleaner cleaner = TestConfig.getCleaner();
    cleaner.setPrimaryKey("id");
    CleanerDriverImpl cleanerDriver = connectedDriver(cleaner, TestConfig.primaryKeys());
    Mockito.when(cleanerDriver.countRows(cleaner.getTable(), cleaner.getWhere())).thenReturn(2);
    Mockito.when(cleanerDriver.getRecords(anyString(), any(), any(), anyInt()))
        .thenReturn(TestConfig.keyRows("1", "2"))
        .thenReturn(Collections.emptyList());
    Mockito.when(cleanerDriver.deleteRecords(anyString(), any(), any(), any(), anyBoolean())).thenReturn(2);

    new ProcessorImpl().process(cleaner, cleanerDriver);

    Mockito.verify(cleanerDriver)
        .deleteRecords(eq(cleaner.getTable()), eq(TestConfig.primaryKeys()), eq(cleaner.getWhere()), any(), eq(true));
    assertThat(cleaner.getPrimaryKey()).isEqualTo("id");
  }

  @Test
  @DisplayName("General processor execution with custom PK that does not exist")
  void testProcessorCustomPKNotExists() throws Exception {
    Cleaner cleaner = TestConfig.getCleaner();
    cleaner.setPrimaryKey("PK");
    CleanerDriverImpl cleanerDriver = connectedDriver(cleaner, TestConfig.primaryKeys());

    assertThatThrownBy(() -> new ProcessorImpl().process(cleaner, cleanerDriver))
        .isInstanceOf(CleanerException.class)
        .hasMessageContaining("Primary key: PK is not valid");

    Mockito.verify(cleanerDriver, Mockito.never()).countRows(anyString(), any());
    Mockito.verify(cleanerDriver, Mockito.never()).deleteRecords(anyString(), any(), any(), any(), anyBoolean());
  }

  @Test
  @DisplayName("General processor execution, do not count rows")
  void testProcessorDoNotCountRows() throws Exception {
    Cleaner cleaner = TestConfig.getCleaner();
    cleaner.setCountRows(false);
    CleanerDriverImpl cleanerDriver = connectedDriver(cleaner, TestConfig.primaryKeys());
    Mockito.when(cleanerDriver.getRecords(anyString(), any(), any(), anyInt()))
        .thenReturn(TestConfig.keyRows("1", "2"))
        .thenReturn(Collections.emptyList());
    Mockito.when(cleanerDriver.deleteRecords(anyString(), any(), any(), any(), anyBoolean())).thenReturn(2);

    new ProcessorImpl().process(cleaner, cleanerDriver);

    Mockito.verify(cleanerDriver, Mockito.never()).countRows(anyString(), any());
    Mockito.verify(cleanerDriver)
        .deleteRecords(eq(cleaner.getTable()), eq(TestConfig.primaryKeys()), eq(cleaner.getWhere()), any(), eq(true));
  }

  @Test
  @DisplayName("General processor execution, quiet mode")
  void testProcessorQuietMode() throws Exception {
    Cleaner cleaner = TestConfig.getCleaner();
    cleaner.setQuiet(true);
    CleanerDriverImpl cleanerDriver = connectedDriver(cleaner, TestConfig.primaryKeys());
    Mockito.when(cleanerDriver.countRows(cleaner.getTable(), cleaner.getWhere())).thenReturn(2);
    Mockito.when(cleanerDriver.getRecords(anyString(), any(), any(), anyInt()))
        .thenReturn(TestConfig.keyRows("1", "2"))
        .thenReturn(Collections.emptyList());
    Mockito.when(cleanerDriver.deleteRecords(anyString(), any(), any(), any(), anyBoolean())).thenReturn(2);

    new ProcessorImpl().process(cleaner, cleanerDriver);

    Mockito.verify(cleanerDriver).countRows(cleaner.getTable(), cleaner.getWhere());
    Mockito.verify(cleanerDriver)
        .deleteRecords(eq(cleaner.getTable()), eq(TestConfig.primaryKeys()), eq(cleaner.getWhere()), any(), eq(true));
  }

  @Test
  @DisplayName("General processor execution, but data exception happens")
  void testProcessorDataException() throws Exception {
    Cleaner cleaner = TestConfig.getCleaner();
    CleanerDriverImpl cleanerDriver = connectedDriver(cleaner, TestConfig.primaryKeys());
    Mockito.when(cleanerDriver.getPrimaryKeys(anyString())).thenThrow(new DataException("Cannot find rows"));

    assertThatThrownBy(() -> new ProcessorImpl().process(cleaner, cleanerDriver))
        .isInstanceOf(CleanerException.class)
        .hasMessageContaining("Failed with an exception: Cannot find rows");

    Mockito.verify(cleanerDriver, Mockito.never()).countRows(anyString(), any());
    Mockito.verify(cleanerDriver, Mockito.never()).deleteRecords(anyString(), any(), any(), any(), anyBoolean());
  }

  @Test
  @DisplayName("General processor execution, no primary keys")
  void testProcessorNoPrimaryKeys() throws Exception {
    Cleaner cleaner = TestConfig.getCleaner();
    CleanerDriverImpl cleanerDriver = connectedDriver(cleaner, Collections.emptyList());

    assertThatThrownBy(() -> new ProcessorImpl().process(cleaner, cleanerDriver))
        .isInstanceOf(CleanerException.class)
        .hasMessageContaining("Cannot find primary keys!");

    Mockito.verify(cleanerDriver, Mockito.never()).countRows(anyString(), any());
    Mockito.verify(cleanerDriver, Mockito.never()).deleteRecords(anyString(), any(), any(), any(), anyBoolean());
  }

  @Test
  @DisplayName("General processor execution, primaryKey parameter empty")
  void testProcessorEmptyCleanerPrimaryKey() throws Exception {
    Cleaner cleaner = TestConfig.getCleaner();
    cleaner.setPrimaryKey("");
    CleanerDriverImpl cleanerDriver = connectedDriver(cleaner, Collections.emptyList());

    assertThatThrownBy(() -> new ProcessorImpl().process(cleaner, cleanerDriver))
        .isInstanceOf(CleanerException.class)
        .hasMessageContaining("Cannot find primary keys!");
  }

  @Test
  @DisplayName("Composite primary key cannot be overridden")
  void testProcessorCompositePrimaryKeyOverride() throws Exception {
    Cleaner cleaner = TestConfig.getCleaner();
    cleaner.setPrimaryKey("ID");
    CleanerDriverImpl cleanerDriver = connectedDriver(cleaner, Arrays.asList("TENANT_ID", "ID"));

    assertThatThrownBy(() -> new ProcessorImpl().process(cleaner, cleanerDriver))
        .isInstanceOf(CleanerException.class)
        .hasMessageContaining("--primary-key cannot override a composite primary key");

    Mockito.verify(cleanerDriver, Mockito.never()).deleteRecords(anyString(), any(), any(), any(), anyBoolean());
  }

  @Test
  @DisplayName("Composite primary key is used when detected")
  void testProcessorCompositePrimaryKey() throws Exception {
    Cleaner cleaner = TestConfig.getCleaner();
    CleanerDriverImpl cleanerDriver = connectedDriver(cleaner, Arrays.asList("TENANT_ID", "ID"));
    List<String> primaryKeys = Arrays.asList("TENANT_ID", "ID");
    List<KeyRow> keyRows = Collections.singletonList(new KeyRow(primaryKeys, Arrays.asList(1, 10)));
    Mockito.when(cleanerDriver.countRows(cleaner.getTable(), cleaner.getWhere())).thenReturn(1);
    Mockito.when(cleanerDriver.getRecords(anyString(), any(), any(), anyInt()))
        .thenReturn(keyRows)
        .thenReturn(Collections.emptyList());
    Mockito.when(cleanerDriver.deleteRecords(anyString(), any(), any(), any(), anyBoolean())).thenReturn(1);

    new ProcessorImpl().process(cleaner, cleanerDriver);

    Mockito.verify(cleanerDriver).deleteRecords(eq(cleaner.getTable()), eq(primaryKeys), eq(cleaner.getWhere()),
        eq(keyRows), eq(true));
    assertThat(cleaner.getPrimaryKey()).isNull();
  }

  @Test
  @DisplayName("Processor uses canonical table name from metadata")
  void testProcessorUsesCanonicalTableName() throws Exception {
    Cleaner cleaner = TestConfig.getCleaner();
    cleaner.setTable("ORDERS");
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    TableMetadata tableInfo = new TableMetadata("orders", "public", null, "TABLE");

    Mockito.doNothing().when(cleanerDriver)
        .connect(anyString(), anyInt(), anyString(), anyString(), anyString());
    Mockito.when(cleanerDriver.isConnected()).thenReturn(true);
    Mockito.when(cleanerDriver.getTable("ORDERS")).thenReturn(Optional.of(tableInfo));
    Mockito.when(cleanerDriver.getPrimaryKeys("public.orders")).thenReturn(TestConfig.primaryKeys());
    Mockito.when(cleanerDriver.countRows("public.orders", cleaner.getWhere())).thenReturn(0);
    Mockito.when(cleanerDriver.getRecords("public.orders", TestConfig.primaryKeys(), cleaner.getWhere(),
            cleaner.getLimit()))
        .thenReturn(Collections.emptyList());

    new ProcessorImpl().process(cleaner, cleanerDriver);

    assertThat(cleaner.getTable()).isEqualTo("ORDERS");
    Mockito.verify(cleanerDriver).getPrimaryKeys("public.orders");
    Mockito.verify(cleanerDriver).getRecords("public.orders", TestConfig.primaryKeys(), cleaner.getWhere(),
        cleaner.getLimit());
  }

  @Test
  @DisplayName("General processor execution with missing table")
  void testProcessorTableNotExist() throws Exception {
    Cleaner cleaner = TestConfig.getCleaner();
    CleanerDriverImpl cleanerDriver = connectedDriver(cleaner, TestConfig.primaryKeys());
    Mockito.when(cleanerDriver.getTable(anyString())).thenReturn(Optional.empty());

    assertThatThrownBy(() -> new ProcessorImpl().process(cleaner, cleanerDriver))
        .isInstanceOf(CleanerException.class)
        .hasMessageContaining("Table testtable does not exist!");

    Mockito.verify(cleanerDriver, Mockito.never()).getPrimaryKeys(anyString());
    Mockito.verify(cleanerDriver, Mockito.never()).deleteRecords(anyString(), any(), any(), any(), anyBoolean());
  }

  @Test
  void testProcessorNoConnection() throws Exception {
    Cleaner cleaner = TestConfig.getCleaner();
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Mockito.doNothing().when(cleanerDriver)
        .connect(anyString(), anyInt(), anyString(), anyString(), anyString());
    Mockito.when(cleanerDriver.isConnected()).thenReturn(false);

    assertThatThrownBy(() -> new ProcessorImpl().process(cleaner, cleanerDriver))
        .isInstanceOf(CleanerException.class)
        .hasMessageContaining("Connection was not established");

    Mockito.verify(cleanerDriver, Mockito.never()).getTable(anyString());
  }

  private CleanerDriverImpl connectedDriver(Cleaner cleaner, List<String> primaryKeys) throws Exception {
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    TableMetadata tableInfo = new TableMetadata(cleaner.getTable(), null, null, "TABLE");

    Mockito.doNothing().when(cleanerDriver)
        .connect(anyString(), anyInt(), anyString(), anyString(), anyString());
    Mockito.when(cleanerDriver.isConnected()).thenReturn(true);
    Mockito.when(cleanerDriver.getTable(anyString())).thenReturn(Optional.of(tableInfo));
    Mockito.when(cleanerDriver.getPrimaryKeys(anyString())).thenReturn(primaryKeys);
    return cleanerDriver;
  }
}
