package com.henrya.tools.htcleaner.processor;

import com.henrya.tools.htcleaner.Cleaner;
import com.henrya.tools.htcleaner.driver.CleanerDriverImpl;
import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.tools.TestConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Timer;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutorImplTest {

  @Test
  @DisplayName("Test executor without a timer")
  void testExecutor() throws DataException {
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());

    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        Arrays.asList("1", "2")).thenReturn(new ArrayList<>());
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn(2);

    ExecutorImpl taskExecutor = new ExecutorImpl(cleaner, cleanerDriver);
    int records = taskExecutor.runTask(new Timer());
    assertThat(records).isEqualTo(2);
  }

  @Test
  @DisplayName("Test executor with a timer")
  void testExecutorWithTimer() throws DataException {
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());

    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        Arrays.asList("1", "2")).thenReturn(new ArrayList<>());
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn(2);

    ExecutorImpl taskExecutor = new ExecutorImpl(cleaner, cleanerDriver);
    int records = taskExecutor.runTask(null);
    assertThat(records).isEqualTo(2);
  }


  @Test
  @DisplayName("Test executor, update error")
  void testExecutorUpdateError() throws DataException {
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());

    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        Arrays.asList("1", "2","3")).thenReturn(new ArrayList<>());
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn(0);

    ExecutorImpl taskExecutor = new ExecutorImpl(cleaner, cleanerDriver);
    int records = taskExecutor.runTask(null);
    assertThat(records).isZero();
  }

  @Test
  @DisplayName("Test executor, exception")
  void testExecutorDataException() throws DataException {
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());

    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenReturn(
        Arrays.asList("1", "2","3"));
    Mockito.when(cleanerDriver.deleteRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenThrow(new DataException("Error occurred"));

    ExecutorImpl taskExecutor = new ExecutorImpl(cleaner, cleanerDriver);
    int records = taskExecutor.runTask(null);
    assertThat(records).isZero();
  }

  @Test
  @DisplayName("Test executor, runtime exception")
  void testExecutorRuntimeException() throws DataException {
    CleanerDriverImpl cleanerDriver = Mockito.mock(CleanerDriverImpl.class);
    Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());

    Mockito.when(cleanerDriver.getRecords(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.anyInt())).thenThrow(new RuntimeException("Error occurred"));

    ExecutorImpl taskExecutor = new ExecutorImpl(cleaner, cleanerDriver);
    int records = taskExecutor.runTask(null);
    assertThat(records).isZero();
  }
}
