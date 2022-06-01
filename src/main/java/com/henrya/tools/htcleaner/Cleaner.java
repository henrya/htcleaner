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

  /**
   * Command line runner main method
   *
   * @param args arguments
   */
  public static void main(String... args) {
    new CommandLine(new Cleaner()).execute(args);
  }

  /**
   * Get spec
   *
   * @return CommandSpec spec
   */
  public CommandSpec getSpec() {
    return spec;
  }

  /**
   * Set CommandSpec
   *
   * @param spec CommandSpec
   */
  public void setSpec(CommandSpec spec) {
    this.spec = spec;
  }

  /**
   * Get host name
   *
   * @return String host name
   */
  public String getHost() {
    return host;
  }

  /**
   * Set host name
   *
   * @param host host name
   */
  public void setHost(String host) {
    this.host = host;
  }

  /**
   * Get user name
   *
   * @return String user name
   */
  public String getUser() {
    return user;
  }

  /**
   * Set user name
   *
   * @param user user name
   */
  public void setUser(String user) {
    this.user = user;
  }

  /**
   * Get password
   *
   * @return String password
   */
  public String getPassword() {
    return password;
  }

  /**
   * Set password
   *
   * @param password Password
   */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * Get port number
   *
   * @return Integer port number
   */
  public Integer getPort() {
    return port;
  }

  /**
   * Set port number
   *
   * @param port port number
   */
  public void setPort(Integer port) {
    this.port = port;
  }

  /**
   * Get database name
   *
   * @return String database name
   */
  public String getDatabase() {
    return database;
  }

  /**
   * Set database name
   *
   * @param database database name
   */
  public void setDatabase(String database) {
    this.database = database;
  }

  /**
   * Get table name
   *
   * @return String table name
   */
  public String getTable() {
    return table;
  }

  /**
   * Set table name
   *
   * @param table Table name
   */
  public void setTable(String table) {
    this.table = table;
  }

  /**
   * Get where statement
   *
   * @return String where
   */
  public String getWhere() {
    return where;
  }

  /**
   * Set where statement
   *
   * @param where Where statement
   */
  public void setWhere(String where) {
    this.where = where;
  }

  /**
   * Get chunk / limit size
   *
   * @return Integer limit
   */
  public Integer getLimit() {
    return limit;
  }

  /**
   * Set chunk / limit size
   *
   * @param limit limit size
   */
  public void setLimit(Integer limit) {
    this.limit = limit;
  }

  /**
   * Get sleep duration
   *
   * @return Integer sleep duration
   */
  public Integer getSleep() {
    return sleep;
  }

  /**
   * Set sleep duration
   *
   * @param sleep sleep duration
   */
  public void setSleep(Integer sleep) {
    this.sleep = sleep;
  }

  /**
   * Get is dry run mode on or off
   *
   * @return boolean Dry run mode
   */
  public boolean isDryRun() {
    return !dryRun;
  }

  /**
   * Set dry run mode
   *
   * @param dryRun dry run mode
   */
  public void setDryRun(boolean dryRun) {
    this.dryRun = dryRun;
  }

  /**
   * Get whether to count rows before the execution or not
   *
   * @return boolean count rows
   */
  public boolean isCountRows() {
    return countRows;
  }

  /**
   * Set whether to count rows before the execution or not
   *
   * @param countRows count rows
   */
  public void setCountRows(boolean countRows) {
    this.countRows = countRows;
  }

  /**
   * Get whether quiet mode is off or not
   *
   * @return boolean quiet mode
   */
  public boolean isNotQuiet() {
    return !quiet;
  }

  /**
   * Set whether quiet mode is off or not
   *
   * @param quiet quiet mode
   */
  public void setQuiet(boolean quiet) {
    this.quiet = quiet;
  }

  /**
   * Get progress delay
   *
   * @return Integer progress delay
   */
  public Integer getProgressDelay() {
    return progressDelay;
  }

  /**
   * Set progress delay
   *
   * @param progressDelay progress delay
   */
  public void setProgressDelay(Integer progressDelay) {
    this.progressDelay = progressDelay;
  }

  /**
   * Get primary key
   *
   * @return String Primary keu
   */
  public String getPrimaryKey() {
    return primaryKey;
  }

  /**
   * Set primary key
   * @param primaryKey primary key
   */
  public void setPrimaryKey(String primaryKey) {
    this.primaryKey = primaryKey;
  }

  /**
   * Return driver name
   *
   * @return String driver
   */
  public String getDriver() {
    return driver;
  }

  /**
   * Set Driver
   *
   * @param driver driver name
   */
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
