package com.henrya.tools.htcleaner.constants;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProcessorConstantsTest {
  @Test
  @DisplayName("Test initialization error")
  void testInitializationError() {
    assertThatThrownBy(ProcessorConstants::new).isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("This class cannot be initialized directly");
  }
}
