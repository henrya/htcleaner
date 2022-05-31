package com.henrya.tools.htcleaner.exception;

/**
 * A cleaner exception, thrown in case of an exception in the business logic
 */
public class CleanerException extends Exception {
  private static final long serialVersionUID = 6633788728766065725L;
  public CleanerException(String errorMessage) {
    super(errorMessage);
  }
}
