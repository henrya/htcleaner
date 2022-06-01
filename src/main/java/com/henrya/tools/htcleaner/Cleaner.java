package com.henrya.tools.htcleaner;

import com.henrya.tools.htcleaner.constants.DefaultsConstants;
import com.henrya.tools.htcleaner.constants.ParameterConstants;
import com.henrya.tools.htcleaner.driver.CleanerDriverImpl;
import com.henrya.tools.htcleaner.exception.CleanerException;
import com.henrya.tools.htcleaner.processor.ProcessorImpl;

import com.henrya.tools.htcleaner.validator.ValidatorImpl;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Cleaner instance
 */
@Command(name = "htcleaner", description = "Clean large tables")
public class Cleaner implements Runnable {

  /**
   * picocli command spec
   */
  @Spec
  CommandSpec spec;

  /**
   * Host name
   */
  @Option(names = {ParameterConstants.PARAMETER_HOST_SHORT,
      ParameterConstants.PARAMETER_HOST_LONG}, required = true, description = "Database host name")
  String host;

  /**
   * User name
   */
  @Option(names = {ParameterConstants.PARAMETER_USER_SHORT,
      ParameterConstants.PARAMETER_USER_LONG}, required = true, description = "Database user name")
  String user;

  /**
   * User password
   */
  @Option(names = {ParameterConstants.PARAMETER_PASSWORD_SHORT,
      ParameterConstants.PARAMETER_PASSWORD_LONG}, required = true, description = "Database password")
  String password;

  /**
   * Database port
   */
  @Option(names = {ParameterConstants.PARAMETER_PORT_SHORT,
      ParameterConstants.PARAMETER_PORT_LONG}, required = true, description = "Port number")
  Integer port;

  /**
   * Database name
   */
  @Option(names = {ParameterConstants.PARAMETER_DATABASE_SHORT,
      ParameterConstants.PARAMETER_DATABASE_LONG}, required = true, description = "Database information")
  String database;

  /**
   * Table name
   */
  @Option(names = {ParameterConstants.PARAMETER_TABLE_SHORT,
      ParameterConstants.PARAMETER_TABLE_LONG}, required = true, description = "Table information")
  String table;

  /**
   * WHERE statement for the query
   */
  @Option(names = {ParameterConstants.PARAMETER_WHERE_SHORT,
      ParameterConstants.PARAMETER_WHERE_LONG}, description = "Where clause when fetching the rows")
  String where;

  /**
   * Chunk size
   */
  @Option(names = {ParameterConstants.PARAMETER_LIMIT_SHORT,
      ParameterConstants.PARAMETER_LIMIT_LONG}, defaultValue = DefaultsConstants.DEFAULT_FETCH_LIMIT, description = "The limit set in each fetch")
  Integer limit;

  /**
   * Interval of the execution
   */
  @Option(names = {ParameterConstants.PARAMETER_SLEEP_SHORT,
      ParameterConstants.PARAMETER_SLEEP_LONG}, defaultValue = DefaultsConstants.DEFAULT_FETCH_SLEEP_MS, description = "Sleep time between fetches")
  Integer sleep;

  /**
   * Dry run mode
   */
  @Option(names = {
      ParameterConstants.PARAMETER_DRY_RUN_LONG}, defaultValue = DefaultsConstants.DEFAULT_DRY_RUN, description = "Performs fetch, but does not execute purge")
  boolean dryRun;

  /**
   * Count amount of the rows to be processed or not
   */
  @Option(names = {
      ParameterConstants.PARAMETER_COUNT_ROWS_LONG}, defaultValue = DefaultsConstants.DEFAULT_COUNT_ROWS, description = "Executes count query to measure progress of the execution")
  boolean countRows;

  /**
   * Display arguments and current progress or not
   */
  @Option(names = {ParameterConstants.PARAMETER_QUIET_SHORT,
      ParameterConstants.PARAMETER_QUIET_LONG}, defaultValue = DefaultsConstants.DEFAULT_QUIET_MODE, description = "Do not print any statistcs")
  boolean quiet;

  /**
   * The interval when progress is being displayed
   */
  @Option(names = {
      ParameterConstants.PARAMETER_PROGRESS_DELAY_LONG}, defaultValue = DefaultsConstants.DEFAULT_PROGRESS_INTERVAL, description = "Interval when progress is updated")
  Integer progressDelay;

  /**
   * Primary key for he query If given, will override they key detected automatically
   */
  @Option(names = {
      ParameterConstants.PARAMETER_PRIMARY_KEY_LONG}, defaultValue = DefaultsConstants.PRIMARY_KEY, description = "Primary key to be used")
  String primaryKey;

  /**
   * Driver name
   */
  @Option(names = {
      ParameterConstants.PARAMETER_DRIVER_LONG}, defaultValue = DefaultsConstants.DEFAULT_DRIVER, description = "JDBC Database driver")
  String driver;

  public static void main(String... args) {
    new CommandLine(new Cleaner()).execute(args);
  }

  public CommandSpec getSpec() {
    return spec;
  }

  public void setSpec(CommandSpec spec) {
    this.spec = spec;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public Integer getPort() {
    return port;
  }

  public void setPort(Integer port) {
    this.port = port;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getWhere() {
    return where;
  }

  public void setWhere(String where) {
    this.where = where;
  }

  public Integer getLimit() {
    return limit;
  }

  public void setLimit(Integer limit) {
    this.limit = limit;
  }

  public Integer getSleep() {
    return sleep;
  }

  public void setSleep(Integer sleep) {
    this.sleep = sleep;
  }

  public boolean isDryRun() {
    return !dryRun;
  }

  public void setDryRun(boolean dryRun) {
    this.dryRun = dryRun;
  }

  public boolean isCountRows() {
    return countRows;
  }

  public void setCountRows(boolean countRows) {
    this.countRows = countRows;
  }

  public boolean isNotQuiet() {
    return !quiet;
  }

  public void setQuiet(boolean quiet) {
    this.quiet = quiet;
  }

  public Integer getProgressDelay() {
    return progressDelay;
  }

  public void setProgressDelay(Integer progressDelay) {
    this.progressDelay = progressDelay;
  }

  public String getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(String primaryKey) {
    this.primaryKey = primaryKey;
  }

  public String getDriver() {
    return driver;
  }

  public void setDriver(String driver) {
    this.driver = driver;
  }

  /**
   * Start the runnable
   */
  public void run() {
    try {
      List<String> arguments = ValidatorImpl.validate(this);
      // print arguments
      if (!arguments.isEmpty()) {
        Logger.getGlobal().info("Arguments: ");
        arguments.forEach(argument -> Logger.getGlobal().info(argument));
      }
      ProcessorImpl processor = new ProcessorImpl();
      processor.process(this, new CleanerDriverImpl(getDriver()));
    } catch (CleanerException e) {
      Logger.getGlobal().log(Level.SEVERE, () ->
          String.format("Execution failed with the error: %s", e.getMessage())
      );
    }
  }
}
