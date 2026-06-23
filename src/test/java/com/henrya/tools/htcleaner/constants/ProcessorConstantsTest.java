package com.henrya.tools.htcleaner.constants;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProcessorConstantsTest {
  @Test
  @DisplayName("Test initialization error")
  void testInitializationError() throws Exception {
    Constructor<ProcessorConstants> constructor = ProcessorConstants.class.getDeclaredConstructor();
    constructor.setAccessible(true);

    assertThatThrownBy(constructor::newInstance)
        .hasRootCauseInstanceOf(UnsupportedOperationException.class)
        .hasRootCauseMessage("This class cannot be initialized directly");
  }
}
