package com.henrya.tools.htcleaner.progress;

/**
 * Progress observer used when progress reporting is disabled.
 */
final class NoOpProgressObserver implements ProgressObserver {

  @Override
  public void setProcessedRows(int processedRows) {
    // Intentionally ignored.
  }

  @Override
  public void close() {
    // No resources to release.
  }
}
