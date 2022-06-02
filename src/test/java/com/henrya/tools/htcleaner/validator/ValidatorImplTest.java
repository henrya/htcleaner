package com.henrya.tools.htcleaner.validator;

import com.henrya.tools.htcleaner.Cleaner;
import com.henrya.tools.htcleaner.tools.TestConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.ParameterException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

class ValidatorImplTest {

  @Test
  @DisplayName("Test basic validator")
  void testValidator() {
    Cleaner cleaner = TestConfig.getCleaner();
    CommandLine commandLine = new CommandLine(cleaner);
    cleaner.setSpec(commandLine.getCommandSpec());
    try {
      List<String> arguments = ValidatorImpl.validate(cleaner);
      assertThat(arguments).hasSize(15);
    } catch (ParameterException e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("Test limit argument error")
  void testLimitArgumentError() throws ParameterException {
      Cleaner cleaner = TestConfig.getCleaner();
      cleaner.setLimit(-1);
      CommandLine commandLine = new CommandLine(cleaner);
      cleaner.setSpec(commandLine.getCommandSpec());
      assertThatThrownBy(() -> ValidatorImpl.validate(cleaner)).isInstanceOf(ParameterException.class)
        .hasMessageContaining("Invalid value '-1' for option '--limit': ");
  }

  @Test
  @DisplayName("Test port argument error")
  void testPortArgumentError() throws ParameterException {
    Cleaner cleaner = TestConfig.getCleaner();
    cleaner.setPort(-1);
    CommandLine commandLine = new CommandLine(cleaner);
    cleaner.setSpec(commandLine.getCommandSpec());
    assertThatThrownBy(() -> ValidatorImpl.validate(cleaner)).isInstanceOf(ParameterException.class)
        .hasMessageContaining("Invalid value '-1' for option '--port': ");
  }

  @Test
  @DisplayName("Test sleep argument error")
  void testSleepArgumentError() throws ParameterException {
    Cleaner cleaner = TestConfig.getCleaner();
    cleaner.setSleep(-1);
    CommandLine commandLine = new CommandLine(cleaner);
    cleaner.setSpec(commandLine.getCommandSpec());
    assertThatThrownBy(() -> ValidatorImpl.validate(cleaner)).isInstanceOf(ParameterException.class)
        .hasMessageContaining("Invalid value '-1' for option '--sleep': ");
  }

  @Test
  @DisplayName("Test progress delay argument error")
  void testProgressDelayArgumentError() throws ParameterException {
    Cleaner cleaner = TestConfig.getCleaner();
    cleaner.setProgressDelay(-1);
    CommandLine commandLine = new CommandLine(cleaner);
    cleaner.setSpec(commandLine.getCommandSpec());
    assertThatThrownBy(() -> ValidatorImpl.validate(cleaner)).isInstanceOf(ParameterException.class)
        .hasMessageContaining("Invalid value '-1' for option '--progress-delay': ");
  }

  @Test
  @DisplayName("Test quiet mode")
  void testQuietMode() {
    Cleaner cleaner = TestConfig.getCleaner();
    cleaner.setQuiet(true);
    CommandLine commandLine = new CommandLine(cleaner);
    cleaner.setSpec(commandLine.getCommandSpec());
    try {
      List<String> arguments = ValidatorImpl.validate(cleaner);
      assertThat(arguments).isEmpty();
    } catch (ParameterException e){
      Assertions.fail();
    }
  }

  @Test
  @DisplayName("Test initialization error")
  void testInitializationError() throws ParameterException {
    assertThatThrownBy(ValidatorImpl::new).isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("This class cannot be initialized directly");
  }
}
