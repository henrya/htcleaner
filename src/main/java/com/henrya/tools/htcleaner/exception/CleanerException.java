package com.henrya.tools.htcleaner.exception;

/**
 * Exception thrown when cleaner orchestration fails.
 */
public class CleanerException extends Exception {
  private static final long serialVersionUID = 6633788728766065725L;

  public CleanerException(String errorMessage) {
    super(errorMessage);
  }

  public CleanerException(String errorMessage, Throwable cause) {
    super(errorMessage, cause);
  }
}
