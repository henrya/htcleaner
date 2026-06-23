package com.henrya.tools.htcleaner.progress;

/**
 * Factory methods for progress observers.
 */
public final class ProgressObservers {

  private ProgressObservers() {
    throw new UnsupportedOperationException("This class cannot be initialized directly");
  }

  /**
   * Returns an observer that ignores progress updates.
   *
   * @return no-op progress observer
   */
  public static ProgressObserver noop() {
    return new NoOpProgressObserver();
  }
}
