package com.henrya.tools.htcleaner.validator;

import com.henrya.tools.htcleaner.Cleaner;
import com.henrya.tools.htcleaner.tools.TestConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.ParameterException;

import java.lang.reflect.Constructor;
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
      assertThat(arguments).hasSize(16);
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
  @DisplayName("Test zero limit argument error")
  void testZeroLimitArgumentError() throws ParameterException {
      Cleaner cleaner = TestConfig.getCleaner();
      cleaner.setLimit(0);
      CommandLine commandLine = new CommandLine(cleaner);
      cleaner.setSpec(commandLine.getCommandSpec());
      assertThatThrownBy(() -> ValidatorImpl.validate(cleaner)).isInstanceOf(ParameterException.class)
          .hasMessageContaining("Invalid value '0' for option '--limit': ");
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
  @DisplayName("Test port upper bound argument error")
  void testPortUpperBoundArgumentError() throws ParameterException {
    Cleaner cleaner = TestConfig.getCleaner();
    cleaner.setPort(65536);
    CommandLine commandLine = new CommandLine(cleaner);
    cleaner.setSpec(commandLine.getCommandSpec());
    assertThatThrownBy(() -> ValidatorImpl.validate(cleaner)).isInstanceOf(ParameterException.class)
        .hasMessageContaining("Invalid value '65536' for option '--port': ");
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
  @DisplayName("Test omitted password is valid")
  void testOmittedPasswordIsValid() {
    Cleaner cleaner = TestConfig.getCleaner();
    cleaner.setPassword(null);
    CommandLine commandLine = new CommandLine(cleaner);
    cleaner.setSpec(commandLine.getCommandSpec());

    assertThat(ValidatorImpl.validate(cleaner)).hasSize(16);
    assertThat(cleaner.getPassword()).isNull();
  }

  @Test
  @DisplayName("Test where predicate argument error")
  void testWherePredicateArgumentError() {
    Cleaner cleaner = TestConfig.getCleaner();
    cleaner.setWhere("WHERE id > 1");
    CommandLine commandLine = new CommandLine(cleaner);
    cleaner.setSpec(commandLine.getCommandSpec());
    assertThatThrownBy(() -> ValidatorImpl.validate(cleaner)).isInstanceOf(ParameterException.class)
        .hasMessageContaining("WHERE predicate must not include the WHERE keyword");
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
  void testInitializationError() throws Exception {
    Constructor<ValidatorImpl> constructor = ValidatorImpl.class.getDeclaredConstructor();
    constructor.setAccessible(true);

    assertThatThrownBy(constructor::newInstance)
        .hasRootCauseInstanceOf(UnsupportedOperationException.class)
        .hasRootCauseMessage("This class cannot be initialized directly");
  }
}
