package com.henrya.tools.htcleaner.processor;

import com.henrya.tools.htcleaner.Cleaner;
import com.henrya.tools.htcleaner.driver.CleanerDriverImpl;
import com.henrya.tools.htcleaner.exception.CleanerException;
import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.model.CleanerConfig;
import com.henrya.tools.htcleaner.model.TableMetadata;
import com.henrya.tools.htcleaner.progress.AsyncProgressObserver;
import com.henrya.tools.htcleaner.progress.ProgressObserver;
import com.henrya.tools.htcleaner.progress.ProgressObservers;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/**
 * Coordinates validation-independent execution after CLI options have been parsed.
 */
public class ProcessorImpl {

  /**
   * Processes a mutable CLI object by first taking an immutable snapshot.
   *
   * @param cleaner parsed CLI command
   * @param cleanerDriver driver instance
   * @throws CleanerException when processing fails
   */
  public void process(@Nonnull Cleaner cleaner, @Nonnull CleanerDriverImpl cleanerDriver) throws CleanerException {
    process(CleanerConfig.from(cleaner), cleanerDriver);
  }

  /**
   * Connects to the configured database, resolves table metadata and primary keys, then runs cleanup batches.
   *
   * @param config immutable cleaner configuration
   * @param cleanerDriver driver instance
   * @throws CleanerException when configuration, metadata, or database execution fails
   */
  public void process(@Nonnull CleanerConfig config, @Nonnull CleanerDriverImpl cleanerDriver) throws CleanerException {
    try {
      cleanerDriver.connect(config.getHost(), config.getPort(),
          config.getDatabase(), config.getUser(),
          config.getPassword());
      if (!cleanerDriver.isConnected()) {
        throw new CleanerException("Connection was not established");
      }

      TableMetadata tableInfo = cleanerDriver.getTable(config.getTable())
          .orElseThrow(() -> new CleanerException(String.format("Table %s does not exist!", config.getTable())));
      CleanerConfig resolvedConfig = config.withTable(tableInfo.qualifiedName());
      List<String> detectedPrimaryKeys = cleanerDriver.getPrimaryKeys(resolvedConfig.getTable());
      List<String> primaryKeys = resolvePrimaryKeys(resolvedConfig, detectedPrimaryKeys);

      Logger.getGlobal().info(() ->
          String.format("Using primary key(s): %s", String.join(", ", primaryKeys))
      );

      processTask(cleanerDriver, resolvedConfig, primaryKeys);
    } catch (DataException e) {
      throw new CleanerException(String.format("Failed with an exception: %s", e.getMessage()), e);
    }
  }

  /**
   * Runs the executor and progress observer for resolved table metadata.
   *
   * @param cleanerDriver CleanerDriverImpl cleanerDriver name
   * @param config Cleaner configuration
   * @param primaryKeys primary key columns
   */
  private void processTask(@Nonnull CleanerDriverImpl cleanerDriver, @Nonnull CleanerConfig config,
      @Nonnull List<String> primaryKeys) throws DataException {

    int totalRows = 0;
    if (config.isCountRows()) {
      totalRows = cleanerDriver.countRows(config.getTable(), config.getWhere());
    }

    Logger.getGlobal().info(() ->
        String.format("Executing with parameters: table %s, primaryKeys: %s, limit: %s",
            config.getTable(), String.join(", ", primaryKeys), config.getLimit())
    );

    try (ProgressObserver progressObserver = createProgressObserver(config, totalRows)) {
      ExecutorImpl taskExecutor = new ExecutorImpl(config, cleanerDriver, progressObserver, primaryKeys);
      int records = taskExecutor.runTask();
      Logger.getGlobal().info(() ->
          String.format("Total records processed: %d", records)
      );
    }
  }

  private ProgressObserver createProgressObserver(CleanerConfig config, int totalRows) {
    if (config.isNotQuiet()) {
      return AsyncProgressObserver.start(totalRows, config.getProgressDelay());
    }
    return ProgressObservers.noop();
  }

  private List<String> resolvePrimaryKeys(CleanerConfig config, List<String> primaryKeys) throws CleanerException {
    if (primaryKeys.isEmpty()) {
      throw new CleanerException("Cannot find primary keys!");
    }

    String requestedPrimaryKey = config.getPrimaryKey();
    if (requestedPrimaryKey != null && !requestedPrimaryKey.isEmpty()) {
      if (primaryKeys.size() > 1) {
        throw new CleanerException("--primary-key cannot override a composite primary key");
      }
      String matchedKey = primaryKeys.stream()
          .filter(primaryKey -> primaryKey.equalsIgnoreCase(requestedPrimaryKey))
          .findFirst()
          .orElseThrow(() -> new CleanerException(
              String.format("Primary key: %s is not valid", requestedPrimaryKey)));
      return Collections.singletonList(matchedKey);
    }

    return primaryKeys;
  }
}
