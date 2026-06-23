package com.henrya.tools.htcleaner.exception;

/**
 * Exception thrown when database access or generated SQL execution fails.
 */
public class DataException extends Exception {
  private static final long serialVersionUID = -4742846613138366488L;

  public DataException(String errorMessage) {
    super(errorMessage);
  }

  public DataException(String errorMessage, Throwable cause) {
    super(errorMessage, cause);
  }
}
