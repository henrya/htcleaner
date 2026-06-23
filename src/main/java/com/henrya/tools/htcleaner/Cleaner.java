package com.henrya.tools.htcleaner;

import com.henrya.tools.htcleaner.cli.ManifestVersionProvider;
import com.henrya.tools.htcleaner.constants.DefaultsConstants;
import com.henrya.tools.htcleaner.constants.ParameterConstants;
import com.henrya.tools.htcleaner.driver.CleanerDriverImpl;
import com.henrya.tools.htcleaner.exception.CleanerException;
import com.henrya.tools.htcleaner.model.CleanerConfig;
import com.henrya.tools.htcleaner.processor.ProcessorImpl;
import com.henrya.tools.htcleaner.validator.ValidatorImpl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Console;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Picocli command that removes rows from a table in bounded batches.
 */
@Command(name = "htcleaner", description = "Nibble rows from large tables in small batches",
    versionProvider = ManifestVersionProvider.class)
public class Cleaner implements Callable<Integer> {

  /**
   * Injected picocli command spec.
   */
  @Spec
  CommandSpec spec;

  private Function<String, char[]> passwordReader = this::readPasswordFromConsole;

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
   * Database password.
   */
  @Option(names = {ParameterConstants.PARAMETER_PASSWORD_SHORT,
      ParameterConstants.PARAMETER_PASSWORD_LONG}, description = "Database password")
  String password;

  /**
   * Prompt for password.
   */
  @Option(names = {
      ParameterConstants.PARAMETER_ASK_PASS_LONG}, description = "Prompt for database password")
  boolean askPass;

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
   * SQL predicate used to filter rows.
   */
  @Option(names = {ParameterConstants.PARAMETER_WHERE_SHORT,
      ParameterConstants.PARAMETER_WHERE_LONG}, required = true,
      description = "SQL predicate without the WHERE keyword; use 1=1 for all rows")
  String where;

  /**
   * Chunk size
   */
  @Option(names = {ParameterConstants.PARAMETER_LIMIT_SHORT,
      ParameterConstants.PARAMETER_LIMIT_LONG}, defaultValue = DefaultsConstants.DEFAULT_FETCH_LIMIT,
      description = "Rows to fetch and purge per batch")
  Integer limit;

  /**
   * Interval of the execution
   */
  @Option(names = {ParameterConstants.PARAMETER_SLEEP_SHORT,
      ParameterConstants.PARAMETER_SLEEP_LONG}, defaultValue = DefaultsConstants.DEFAULT_FETCH_SLEEP_MS,
      description = "Sleep time between fetches")
  Integer sleep;

  /**
   * Dry run mode
   */
  @Option(names = {
      ParameterConstants.PARAMETER_DRY_RUN_LONG}, defaultValue = DefaultsConstants.DEFAULT_DRY_RUN,
      description = "Fetch one batch, but do not purge")
  boolean dryRun;

  /**
   * Whether to count rows before execution.
   */
  @Option(names = {
      ParameterConstants.PARAMETER_COUNT_ROWS_LONG}, defaultValue = DefaultsConstants.DEFAULT_COUNT_ROWS,
      description = "Count matching rows before execution")
  boolean countRows;

  /**
   * Display arguments and current progress or not
   */
  @Option(names = {ParameterConstants.PARAMETER_QUIET_SHORT,
      ParameterConstants.PARAMETER_QUIET_LONG}, defaultValue = DefaultsConstants.DEFAULT_QUIET_MODE,
      description = "Do not print arguments or progress")
  boolean quiet;

  /**
   * Progress display interval in milliseconds.
   */
  @Option(names = {
      ParameterConstants.PARAMETER_PROGRESS_DELAY_LONG}, defaultValue = DefaultsConstants.DEFAULT_PROGRESS_INTERVAL,
      description = "Progress update interval in milliseconds")
  Integer progressDelay;

  /**
   * Primary key override. By default, the cleaner detects the table primary key from metadata.
   */
  @Option(names = {
      ParameterConstants.PARAMETER_PRIMARY_KEY_LONG}, defaultValue = DefaultsConstants.PRIMARY_KEY,
      description = "Single-column primary key override")
  String primaryKey;

  /**
   * Driver name
   */
  @Option(names = {
      ParameterConstants.PARAMETER_DRIVER_LONG}, defaultValue = DefaultsConstants.DEFAULT_DRIVER,
      description = "JDBC database driver")
  String driver;

  /**
   * Display command help.
   */
  @Option(names = {ParameterConstants.PARAMETER_HELP_LONG}, usageHelp = true, description = "Show this help and exit")
  boolean usageHelpRequested;

  /**
   * Display command version.
   */
  @Option(names = {
      ParameterConstants.PARAMETER_VERSION_LONG}, versionHelp = true, description = "Show version and exit")
  boolean versionHelpRequested;

  /**
   * Execute command line and return exit code.
   *
   * @param args command line arguments
   * @return command exit code
   */
  public static int execute(String... args) {
    return new CommandLine(new Cleaner()).execute(args);
  }

  /**
   * Get spec
   *
   * @return CommandSpec spec
   */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP",
      justification = "Picocli owns and mutates CommandSpec; Cleaner only exposes it for validation.")
  public CommandSpec getSpec() {
    return spec;
  }

  /**
   * Set CommandSpec
   *
   * @param spec CommandSpec
   */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2",
      justification = "Picocli injects CommandSpec; copying is not supported by the framework.")
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
   * Returns the configured password.
   *
   * @return String password
   */
  public String getPassword() {
    return password;
  }

  /**
   * Overrides password prompt handling. Intended for tests.
   *
   * @param passwordReader password prompt function
   */
  void setPasswordReader(Function<String, char[]> passwordReader) {
    this.passwordReader = Objects.requireNonNull(passwordReader, "passwordReader");
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
   * Returns whether password prompting is enabled.
   *
   * @return boolean ask password
   */
  public boolean isAskPass() {
    return askPass;
  }

  /**
   * Set password prompt mode.
   *
   * @param askPass prompt for password
   */
  public void setAskPass(boolean askPass) {
    this.askPass = askPass;
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
    return dryRun;
  }

  /**
   * Returns whether SQL changes should be committed.
   *
   * @return boolean commit changes
   */
  public boolean shouldCommit() {
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
   * @return String Primary key
   */
  public String getPrimaryKey() {
    return primaryKey;
  }

  /**
   * Set primary key
   *
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
   * Set driver
   *
   * @param driver driver name
   */
  public void setDriver(String driver) {
    this.driver = driver;
  }

  private void resolvePassword() {
    if (password != null || !askPass) {
      return;
    }

    char[] promptedPassword = passwordReader.apply("Enter password: ");
    if (promptedPassword == null) {
      throw new CommandLine.ParameterException(spec.commandLine(),
          "Cannot read password from console for --ask-pass");
    }

    try {
      password = new String(promptedPassword);
    } finally {
      Arrays.fill(promptedPassword, '\0');
    }
  }

  private char[] readPasswordFromConsole(String prompt) {
    Console console = System.console();
    if (console == null) {
      return null;
    }
    return console.readPassword("%s", prompt);
  }

  /**
   * Executes the cleaner command and returns a picocli exit code.
   *
   * @return command exit code
   */
  public Integer call() {
    try {
      resolvePassword();
      List<String> arguments = ValidatorImpl.validate(this);
      if (!arguments.isEmpty()) {
        Logger.getGlobal().info("Arguments: ");
        arguments.forEach(argument -> Logger.getGlobal().info(argument));
      }
      CleanerConfig config = CleanerConfig.from(this);
      try (CleanerDriverImpl cleanerDriver = new CleanerDriverImpl(getDriver())) {
        ProcessorImpl processor = new ProcessorImpl();
        processor.process(config, cleanerDriver);
      }
      return CommandLine.ExitCode.OK;
    } catch (CommandLine.ParameterException e) {
      spec.commandLine().getErr().println(e.getMessage());
      spec.commandLine().usage(spec.commandLine().getErr());
      return CommandLine.ExitCode.USAGE;
    } catch (CleanerException e) {
      Logger.getGlobal().log(Level.SEVERE, String.format("Execution failed with the error: %s", e.getMessage()), e);
      return CommandLine.ExitCode.SOFTWARE;
    }
  }
}
