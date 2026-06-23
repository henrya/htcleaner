package com.henrya.tools.htcleaner.progress;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncProgressObserverTest {

  @Test
  @DisplayName("Progress observer reports independently while processing thread waits")
  void reportsIndependentlyFromCallerThread() throws Exception {
    CountDownLatch progressReported = new CountDownLatch(1);

    try (ProgressObserver observer = AsyncProgressObserver.start(10, 10, message -> {
      if (message.startsWith("Total progress")) {
        progressReported.countDown();
      }
    })) {
      assertThat(progressReported.await(1, TimeUnit.SECONDS)).isTrue();
      observer.setProcessedRows(5);
    }
  }

  @Test
  @DisplayName("Progress observer keeps original zero-row reporting behavior")
  void keepsOriginalZeroRowReportingBehavior() throws Exception {
    AtomicInteger messages = new AtomicInteger();
    AtomicReference<String> lastMessage = new AtomicReference<>();

    try (ProgressObserver observer = AsyncProgressObserver.start(0, 10, message -> {
      messages.incrementAndGet();
      lastMessage.set(message);
    })) {
      observer.setProcessedRows(5);
      Thread.sleep(50);
    }

    assertThat(messages).hasValue(1);
    assertThat(lastMessage).hasValue("Total rows found: 0");
  }

  @Test
  @DisplayName("Progress observer keeps original percentage format")
  void keepsOriginalPercentageFormat() throws Exception {
    CountDownLatch progressReported = new CountDownLatch(1);
    AtomicReference<String> progressMessage = new AtomicReference<>();

    try (ProgressObserver observer = AsyncProgressObserver.start(10, 10, message -> {
      if ("Total progress: 50.00% [5 of 10]".equals(message)) {
        progressMessage.set(message);
        progressReported.countDown();
      }
    })) {
      observer.setProcessedRows(5);
      assertThat(progressReported.await(1, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(progressMessage.get()).isEqualTo("Total progress: 50.00% [5 of 10]");
  }
}
