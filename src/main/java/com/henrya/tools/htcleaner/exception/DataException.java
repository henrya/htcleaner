package com.henrya.tools.htcleaner.exception;

/**
 * A data exception, thrown when a low level java.sql.SQLException happens
 */
public class DataException extends Exception {
    private static final long serialVersionUID = -4742846613138366488L;
    public DataException(String errorMessage) {
        super(errorMessage);
    }
}
