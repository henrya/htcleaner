package com.henrya.tools.htcleaner.constants;

/**
 * Default CLI option values.
 */
public final class DefaultsConstants {
  /**
   * Default sleep between batches in milliseconds.
   */
  public static final String DEFAULT_FETCH_SLEEP_MS = "1000";

  /**
   * Default fetch size for each batch.
   */
  public static final String DEFAULT_FETCH_LIMIT = "1000";

  /**
   * Dry run is disabled by default.
   */
  public static final String DEFAULT_DRY_RUN = "false";

  /**
   * Verbose output is enabled by default.
   */
  public static final String DEFAULT_QUIET_MODE = "false";

  /**
   * Default progress display interval in milliseconds.
   */
  public static final String DEFAULT_PROGRESS_INTERVAL = "10000";

  /**
   * Row counting is enabled by default.
   */
  public static final String DEFAULT_COUNT_ROWS = "true";

  /**
   * Default JDBC driver.
   */
  public static final String DEFAULT_DRIVER = "mysql";

  /**
   * Empty primary-key override means "detect from metadata".
   */
  public static final String PRIMARY_KEY = "";

  private DefaultsConstants() {
    throw new UnsupportedOperationException("This class cannot be initialized directly");
  }
}
