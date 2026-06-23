package com.henrya.tools.htcleaner.progress;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * Reports progress from a daemon scheduler independent of batch processing.
 */
public final class AsyncProgressObserver implements ProgressObserver {
  private final AtomicInteger processedRows = new AtomicInteger(0);
  private final ScheduledExecutorService executorService;
  private final Consumer<String> reporter;
  private final int totalRows;

  private AsyncProgressObserver(int totalRows, ScheduledExecutorService executorService, Consumer<String> reporter) {
    this.totalRows = totalRows;
    this.executorService = executorService;
    this.reporter = reporter;
  }

  /**
   * Starts asynchronous progress reporting.
   *
   * @param totalRows total rows expected for the run
   * @param progressDelay interval in milliseconds between progress reports
   * @return observer to update and close when processing ends
   */
  public static ProgressObserver start(int totalRows, int progressDelay) {
    return start(totalRows, progressDelay, message -> Logger.getGlobal().info(message));
  }

  static ProgressObserver start(int totalRows, int progressDelay, Consumer<String> reporter) {
    reporter.accept(String.format("Total rows found: %d", totalRows));
    if (totalRows <= 0) {
      return ProgressObservers.noop();
    }

    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread thread = new Thread(runnable, "htcleaner-progress");
      thread.setDaemon(true);
      return thread;
    });
    AsyncProgressObserver observer = new AsyncProgressObserver(totalRows, executorService, reporter);
    executorService.scheduleAtFixedRate(observer::reportProgress, progressDelay, progressDelay,
        TimeUnit.MILLISECONDS);
    return observer;
  }

  @Override
  public void setProcessedRows(int processedRows) {
    this.processedRows.set(processedRows);
  }

  @Override
  public void close() {
    executorService.shutdownNow();
  }

  private void reportProgress() {
    float progress = processedRows.get();
    float percentage = progress / totalRows * 100;
    reporter.accept(String.format("Total progress: %.2f%% [%.0f of %d]", percentage, progress, totalRows));
  }
}
