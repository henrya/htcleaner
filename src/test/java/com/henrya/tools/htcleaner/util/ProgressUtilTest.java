package com.henrya.tools.htcleaner.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.Timer;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;

import static org.assertj.core.api.Assertions.assertThat;

class ProgressUtilTest {

  @Test
  @DisplayName("Progress utility test")
  void testProgressUtil() {
    ProgressUtil progressUtil = new ProgressUtil();
    progressUtil.setTotalRows(20);
    Timer timer = progressUtil.displayProgress(100);
    for (int i = 0; i <= 20; i++) {
      progressUtil.setProcessedRows(i);
      Awaitility.await().atMost(110, TimeUnit.MILLISECONDS)
          .untilAsserted(() -> assertThat(timer).isNotNull());
    }
  }

  @Test
  @DisplayName("Progress utility test without rows")
  void testProgressUtilZeroRows() {
    ProgressUtil progressUtil = new ProgressUtil();
    progressUtil.setTotalRows(0);
    Timer timer = progressUtil.displayProgress(100);
    for (int i = 0; i <= 20; i++) {
      progressUtil.setProcessedRows(i);
      Awaitility.await().atMost(110, TimeUnit.MILLISECONDS)
          .untilAsserted(() -> assertThat(timer).isNull());
    }
  }
}
