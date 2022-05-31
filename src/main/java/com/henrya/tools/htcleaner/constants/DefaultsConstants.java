package com.henrya.tools.htcleaner.constants;

/**
 * Default constants
 */
public class DefaultsConstants {
  DefaultsConstants(){
    throw new UnsupportedOperationException("This class cannot be initialized directly");
  }

  /**
   * Default sleep
   */
  public static final String DEFAULT_FETCH_SLEEP_MS = "1000";

  /**
   * Default limit for each fetch
   */
  public static final String DEFAULT_FETCH_LIMIT = "1000";

  /**
   * Is the dry run option disabled by default or not
   */
  public static final String DEFAULT_DRY_RUN = "false";

  /**
   * Default status for verbose mode
   */
  public static final String DEFAULT_QUIET_MODE = "false";

  /**
   * Default interval when displaying the progress
   */
  public static final String DEFAULT_PROGRESS_INTERVAL = "10000";

  /**
   * Count rows by default or not
   */
  public static final String DEFAULT_COUNT_ROWS = "true";

  /**
   * Default driver
   */
  public static final String DEFAULT_DRIVER = "mysql";

  /**
   * Default primary key
   */
  public static final String PRIMARY_KEY = "";
}
