package com.henrya.tools.htcleaner.constants;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ParameterConstantsTest {
  @Test
  @DisplayName("Delete bind parameter limit is defined with CLI parameter constants")
  void testDeleteBindParameterLimit() {
    assertThat(ParameterConstants.MAX_DELETE_BIND_PARAMETERS).isEqualTo(10_000);
  }

  @Test
  @DisplayName("Ask pass parameter is defined")
  void testAskPassParameter() {
    assertThat(ParameterConstants.PARAMETER_ASK_PASS_LONG).isEqualTo("--ask-pass");
  }

  @Test
  @DisplayName("Help and version parameters are defined")
  void testHelpAndVersionParameters() {
    assertThat(ParameterConstants.PARAMETER_HELP_LONG).isEqualTo("--help");
    assertThat(ParameterConstants.PARAMETER_VERSION_LONG).isEqualTo("--version");
  }

  @Test
  @DisplayName("Test initialization error")
  void testInitializationError() throws Exception {
    Constructor<ParameterConstants> constructor = ParameterConstants.class.getDeclaredConstructor();
    constructor.setAccessible(true);

    assertThatThrownBy(constructor::newInstance)
        .hasRootCauseInstanceOf(UnsupportedOperationException.class)
        .hasRootCauseMessage("This class cannot be initialized directly");
  }
}
