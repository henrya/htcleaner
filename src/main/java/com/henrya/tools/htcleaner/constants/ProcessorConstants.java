package com.henrya.tools.htcleaner.constants;

/**
 * Constants being used in the processor
 */
public class ProcessorConstants {
  ProcessorConstants(){
    throw new UnsupportedOperationException("This class cannot be initialized directly");
  }

  /**
   * Mysql connection URL
   */
  public static final String CONN_URI_MYSQL = "jdbc:mysql://%s:%s/%s";

  /**
   * H2DB connection url
   */
  public static final String CONN_URI_H2 = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;TRACE_LEVEL_FILE=1;TRACE_LEVEL_SYSTEM_OUT=1";

  /**
   *  Oracle connection url
   */
  public static final String CONN_URI_ORACLE = "jdbc:oracle:thin:@//%s:%s/%s";

  /**
   * * Postgres connection url
   */
  public static final String CONN_URI_POSTGRES = "jdbc:postgresql://%s:%s/%s";

  /**
   *  Maximum amount of errors allowed to happen in a row
   */
  public static final int MAX_TASK_ERRORS = 2;
}
