package com.henrya.tools.htcleaner.progress;

/**
 * Receives progress updates from the sequential batch executor.
 */
public interface ProgressObserver extends AutoCloseable {

  /**
   * Updates the number of processed rows.
   *
   * @param processedRows processed row count
   */
  void setProcessedRows(int processedRows);

  @Override
  void close();
}
