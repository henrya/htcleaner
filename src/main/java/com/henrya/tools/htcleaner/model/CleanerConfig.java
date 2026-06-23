package com.henrya.tools.htcleaner.model;

import com.henrya.tools.htcleaner.Cleaner;

import java.util.Objects;

/**
 * Immutable execution configuration derived from CLI input.
 */
public final class CleanerConfig {
  private final String host;
  private final Integer port;
  private final String database;
  private final String user;
  private final String password;
  private final String table;
  private final String where;
  private final Integer limit;
  private final Integer sleep;
  private final boolean dryRun;
  private final boolean countRows;
  private final boolean quiet;
  private final Integer progressDelay;
  private final String primaryKey;
  private final String driver;

  private CleanerConfig(Builder builder) {
    this.host = builder.host;
    this.port = builder.port;
    this.database = builder.database;
    this.user = builder.user;
    this.password = builder.password;
    this.table = builder.table;
    this.where = builder.where;
    this.limit = builder.limit;
    this.sleep = builder.sleep;
    this.dryRun = builder.dryRun;
    this.countRows = builder.countRows;
    this.quiet = builder.quiet;
    this.progressDelay = builder.progressDelay;
    this.primaryKey = builder.primaryKey;
    this.driver = builder.driver;
  }

  /**
   * Creates an immutable snapshot from a parsed CLI object.
   *
   * @param cleaner parsed CLI command
   * @return immutable cleaner configuration
   */
  public static CleanerConfig from(Cleaner cleaner) {
    Objects.requireNonNull(cleaner, "cleaner");
    return new Builder()
        .host(cleaner.getHost())
        .port(cleaner.getPort())
        .database(cleaner.getDatabase())
        .user(cleaner.getUser())
        .password(cleaner.getPassword())
        .table(cleaner.getTable())
        .where(cleaner.getWhere())
        .limit(cleaner.getLimit())
        .sleep(cleaner.getSleep())
        .dryRun(cleaner.isDryRun())
        .countRows(cleaner.isCountRows())
        .quiet(!cleaner.isNotQuiet())
        .progressDelay(cleaner.getProgressDelay())
        .primaryKey(cleaner.getPrimaryKey())
        .driver(cleaner.getDriver())
        .build();
  }

  /**
   * Returns a copy with the table name resolved from database metadata.
   *
   * @param table canonical table name
   * @return copied cleaner configuration
   */
  public CleanerConfig withTable(String table) {
    return toBuilder().table(Objects.requireNonNull(table, "table")).build();
  }

  private Builder toBuilder() {
    return new Builder()
        .host(host)
        .port(port)
        .database(database)
        .user(user)
        .password(password)
        .table(table)
        .where(where)
        .limit(limit)
        .sleep(sleep)
        .dryRun(dryRun)
        .countRows(countRows)
        .quiet(quiet)
        .progressDelay(progressDelay)
        .primaryKey(primaryKey)
        .driver(driver);
  }

  public String getHost() {
    return host;
  }

  public Integer getPort() {
    return port;
  }

  public String getDatabase() {
    return database;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public String getTable() {
    return table;
  }

  public String getWhere() {
    return where;
  }

  public Integer getLimit() {
    return limit;
  }

  public Integer getSleep() {
    return sleep;
  }

  public boolean isDryRun() {
    return dryRun;
  }

  public boolean isCountRows() {
    return countRows;
  }

  public boolean isNotQuiet() {
    return !quiet;
  }

  public Integer getProgressDelay() {
    return progressDelay;
  }

  public String getPrimaryKey() {
    return primaryKey;
  }

  public String getDriver() {
    return driver;
  }

  public boolean shouldCommit() {
    return !dryRun;
  }

  private static final class Builder {
    private String host;
    private Integer port;
    private String database;
    private String user;
    private String password;
    private String table;
    private String where;
    private Integer limit;
    private Integer sleep;
    private boolean dryRun;
    private boolean countRows;
    private boolean quiet;
    private Integer progressDelay;
    private String primaryKey;
    private String driver;

    private Builder host(String host) {
      this.host = host;
      return this;
    }

    private Builder port(Integer port) {
      this.port = port;
      return this;
    }

    private Builder database(String database) {
      this.database = database;
      return this;
    }

    private Builder user(String user) {
      this.user = user;
      return this;
    }

    private Builder password(String password) {
      this.password = password;
      return this;
    }

    private Builder table(String table) {
      this.table = table;
      return this;
    }

    private Builder where(String where) {
      this.where = where;
      return this;
    }

    private Builder limit(Integer limit) {
      this.limit = limit;
      return this;
    }

    private Builder sleep(Integer sleep) {
      this.sleep = sleep;
      return this;
    }

    private Builder dryRun(boolean dryRun) {
      this.dryRun = dryRun;
      return this;
    }

    private Builder countRows(boolean countRows) {
      this.countRows = countRows;
      return this;
    }

    private Builder quiet(boolean quiet) {
      this.quiet = quiet;
      return this;
    }

    private Builder progressDelay(Integer progressDelay) {
      this.progressDelay = progressDelay;
      return this;
    }

    private Builder primaryKey(String primaryKey) {
      this.primaryKey = primaryKey;
      return this;
    }

    private Builder driver(String driver) {
      this.driver = driver;
      return this;
    }

    private CleanerConfig build() {
      return new CleanerConfig(this);
    }
  }
}
