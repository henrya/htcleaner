package com.henrya.tools.htcleaner.processor;

import com.henrya.tools.htcleaner.Cleaner;
import com.henrya.tools.htcleaner.driver.CleanerDriverImpl;
import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.model.CleanerConfig;
import com.henrya.tools.htcleaner.tools.TestConfig;
import com.henrya.tools.htcleaner.progress.ProgressObservers;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutorImplTest {

  @Test
  @DisplayName("Test executor with no-op progress observer")
  void testExecutor() throws DataException {
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());

    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        TestConfig.keyRows("1", "2")).thenReturn(new ArrayList<>());
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn(2);

    ExecutorImpl taskExecutor = new ExecutorImpl(CleanerConfig.from(cleaner), cleanerDriver, ProgressObservers.noop(),
        TestConfig.primaryKeys());
    int records = taskExecutor.runTask();
    assertThat(records).isEqualTo(2);
  }

  @Test
  @DisplayName("Test executor completes when no records remain")
  void testExecutorCompletesWhenNoRecordsRemain() throws DataException {
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());

    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        TestConfig.keyRows("1", "2")).thenReturn(new ArrayList<>());
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn(2);

    ExecutorImpl taskExecutor = new ExecutorImpl(CleanerConfig.from(cleaner), cleanerDriver, ProgressObservers.noop(),
        TestConfig.primaryKeys());
    int records = taskExecutor.runTask();
    assertThat(records).isEqualTo(2);
  }


  @Test
  @DisplayName("Test executor, update error")
  void testExecutorUpdateError() throws DataException {
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());

    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        TestConfig.keyRows("1", "2","3")).thenReturn(new ArrayList<>());
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn(0);

    ExecutorImpl taskExecutor = new ExecutorImpl(CleanerConfig.from(cleaner), cleanerDriver, ProgressObservers.noop(),
        TestConfig.primaryKeys());
    int records = taskExecutor.runTask();
    assertThat(records).isZero();
  }

  @Test
  @DisplayName("Dry run fetches one batch without deleting")
  void testExecutorDryRunDoesNotDelete() throws DataException {
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());
    cleaner.setDryRun(true);

    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        TestConfig.keyRows("1", "2"));

    ExecutorImpl taskExecutor = new ExecutorImpl(CleanerConfig.from(cleaner), cleanerDriver, ProgressObservers.noop(),
        TestConfig.primaryKeys());
    int records = taskExecutor.runTask();

    assertThat(records).isEqualTo(2);
    Mockito.verify(cleanerDriver, Mockito.times(1))
        .getRecords(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt());
    Mockito.verify(cleanerDriver, Mockito.never())
        .deleteRecords(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());
  }

  @Test
  @DisplayName("Repeated zero updates fail when rows remain fetchable")
  void testExecutorRepeatedZeroUpdatesFail() throws DataException {
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());

    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        TestConfig.keyRows("1", "2","3"));
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn(0);

    ExecutorImpl taskExecutor = new ExecutorImpl(CleanerConfig.from(cleaner), cleanerDriver, ProgressObservers.noop(),
        TestConfig.primaryKeys());
    org.assertj.core.api.Assertions.assertThatThrownBy(taskExecutor::runTask)
        .isInstanceOf(DataException.class)
        .hasMessageContaining("repeated update errors");
  }

  @Test
  @DisplayName("Executor restores interrupt status")
  void testExecutorInterrupt() throws DataException {
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());

    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        TestConfig.keyRows("1"));
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn(1);

    ExecutorImpl taskExecutor = new ExecutorImpl(CleanerConfig.from(cleaner), cleanerDriver, ProgressObservers.noop(),
        TestConfig.primaryKeys());
    try {
      Thread.currentThread().interrupt();
      org.assertj.core.api.Assertions.assertThatThrownBy(taskExecutor::runTask)
          .isInstanceOf(DataException.class)
          .hasMessageContaining("Execution was interrupted");
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  @DisplayName("Test executor, exception")
  void testExecutorDataException() throws DataException {
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());

    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        TestConfig.keyRows("1", "2","3"));
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenThrow(new DataException("Error occurred"));

    ExecutorImpl taskExecutor = new ExecutorImpl(CleanerConfig.from(cleaner), cleanerDriver, ProgressObservers.noop(),
        TestConfig.primaryKeys());
    org.assertj.core.api.Assertions.assertThatThrownBy(taskExecutor::runTask)
        .isInstanceOf(DataException.class);
  }

  @Test
  @DisplayName("Test executor, runtime exception")
  void testExecutorRuntimeException() throws DataException {
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());

    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenThrow(new RuntimeException("Error occurred"));

    ExecutorImpl taskExecutor = new ExecutorImpl(CleanerConfig.from(cleaner), cleanerDriver, ProgressObservers.noop(),
        TestConfig.primaryKeys());
    org.assertj.core.api.Assertions.assertThatThrownBy(taskExecutor::runTask)
        .isInstanceOf(RuntimeException.class);
  }
}
