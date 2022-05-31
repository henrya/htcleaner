package com.henrya.tools.htcleaner.processor;

import com.henrya.tools.htcleaner.Cleaner;
import com.henrya.tools.htcleaner.driver.CleanerDriverImpl;
import com.henrya.tools.htcleaner.exception.CleanerException;
import com.henrya.tools.htcleaner.tools.DataCreator;
import com.henrya.tools.htcleaner.tools.TestConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ITProcessorImplTest {

  @Test
  @DisplayName("Test processor without where")
  void processTest() throws Exception {
    try {
      DataCreator.createData(TestConfig.TABLE_NAME, 9999);
      ProcessorImpl processor = new ProcessorImpl();
      Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());
      CleanerDriverImpl cleanerDriver = Mockito.spy(new CleanerDriverImpl(cleaner.getDriver()));
      processor.process(cleaner, cleanerDriver);
      Mockito.verify(cleaner, Mockito.times(1)).getDriver();
      Mockito.verify(cleaner, Mockito.times(1)).getSleep();
      Mockito.verify(cleanerDriver, Mockito.times(1)).getPrimaryKeys(Mockito.anyString());
      Mockito.verify(cleanerDriver, Mockito.times(1)).getTable(Mockito.anyString());
    } catch (CleanerException e) {
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("Test processor with where")
  void processWithWhereTest() throws Exception {
    try {
      DataCreator.createData(TestConfig.TABLE_NAME + "_WHERE", 9999);
      ProcessorImpl processor = new ProcessorImpl();
      Cleaner cleaner = Mockito.spy(TestConfig.getCleaner());
      cleaner.setTable(TestConfig.TABLE_NAME + "_WHERE");
      cleaner.setWhere("a LIKE 'a%'");
      CleanerDriverImpl cleanerDriver = Mockito.spy(new CleanerDriverImpl(cleaner.getDriver()));
      processor.process(cleaner, cleanerDriver);
      Mockito.verify(cleaner, Mockito.times(1)).getDriver();
      Mockito.verify(cleaner, Mockito.times(1)).getSleep();
      Mockito.verify(cleanerDriver, Mockito.times(1)).getPrimaryKeys(Mockito.anyString());
      Mockito.verify(cleanerDriver, Mockito.times(1)).getTable(Mockito.anyString());
    }catch (CleanerException e){
      Assertions.fail();
    }
  }
}
