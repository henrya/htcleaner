package com.henrya.tools.htcleaner.constants;

/**
 * JDBC and processor execution constants.
 */
public final class ProcessorConstants {
  /**
   * MySQL connection URL.
   */
  public static final String CONN_URI_MYSQL = "jdbc:mysql://%s:%s/%s";

  /**
   * H2 in-memory connection URL.
   */
  public static final String CONN_URI_H2 = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;TRACE_LEVEL_FILE=1;TRACE_LEVEL_SYSTEM_OUT=1";

  /**
   * Oracle connection URL.
   */
  public static final String CONN_URI_ORACLE = "jdbc:oracle:thin:@//%s:%s/%s";

  /**
   * PostgreSQL connection URL.
   */
  public static final String CONN_URI_POSTGRES = "jdbc:postgresql://%s:%s/%s";

  /**
   * Maximum consecutive zero-update errors before aborting execution.
   */
  public static final int MAX_TASK_ERRORS = 2;

  private ProcessorConstants() {
    throw new UnsupportedOperationException("This class cannot be initialized directly");
  }
}
