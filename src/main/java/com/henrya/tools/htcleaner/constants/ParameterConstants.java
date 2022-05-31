package com.henrya.tools.htcleaner.constants;

/**
 * Application parameter constants
 */
public class ParameterConstants {
    ParameterConstants(){
        throw new UnsupportedOperationException("This class cannot be initialized directly");
    }
    public static final String PARAMETER_HOST_LONG = "--host";
    public static final String PARAMETER_HOST_SHORT = "-h";
    public static final String PARAMETER_USER_LONG = "--user";
    public static final String PARAMETER_USER_SHORT = "--u";
    public static final String PARAMETER_PASSWORD_LONG = "--password";
    public static final String PARAMETER_PASSWORD_SHORT = "-p";
    public static final String PARAMETER_PORT_LONG = "--port";
    public static final String PARAMETER_PORT_SHORT = "-P";
    public static final String PARAMETER_DATABASE_LONG = "--database";
    public static final String PARAMETER_DATABASE_SHORT = "-d";
    public static final String PARAMETER_TABLE_LONG = "--table";
    public static final String PARAMETER_TABLE_SHORT = "-t";
    public static final String PARAMETER_WHERE_LONG = "--where";
    public static final String PARAMETER_WHERE_SHORT = "-w";
    public static final String PARAMETER_LIMIT_LONG = "--limit";
    public static final String PARAMETER_LIMIT_SHORT = "-l";
    public static final String PARAMETER_SLEEP_LONG = "--sleep";
    public static final String PARAMETER_SLEEP_SHORT = "-s";
    public static final String PARAMETER_DRY_RUN_LONG = "--dry-run";
    public static final String PARAMETER_COUNT_ROWS_LONG = "--count-rows";
    public static final String PARAMETER_QUIET_LONG = "--quiet";
    public static final String PARAMETER_QUIET_SHORT = "-q";
    public static final String PARAMETER_PROGRESS_DELAY_LONG = "--progress-delay";
    public static final String PARAMETER_PRIMARY_KEY_LONG = "--primary-key";
    public static final String PARAMETER_DRIVER_LONG = "--driver";
}