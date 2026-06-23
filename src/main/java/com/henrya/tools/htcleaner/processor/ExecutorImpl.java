package com.henrya.tools.htcleaner.processor;

import com.henrya.tools.htcleaner.constants.ProcessorConstants;
import com.henrya.tools.htcleaner.driver.CleanerDriverImpl;
import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.model.CleanerConfig;
import com.henrya.tools.htcleaner.model.KeyRow;
import com.henrya.tools.htcleaner.progress.ProgressObserver;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * Executes fetch/delete batches for a resolved cleaner configuration.
 */
public final class ExecutorImpl {

  private final CleanerConfig config;
  private final CleanerDriverImpl driver;
  private final ProgressObserver progressObserver;
  private final List<String> primaryKeys;

  /**
   * Creates an executor for a single cleaner run.
   *
   * @param config cleaner configuration
   * @param cleanerDriver connected database driver
   * @param progressObserver progress observer
   * @param primaryKeys primary-key columns to fetch and delete by
   */
  public ExecutorImpl(@Nonnull CleanerConfig config, @Nonnull CleanerDriverImpl cleanerDriver,
      @Nonnull ProgressObserver progressObserver, @Nonnull List<String> primaryKeys) {
    this.config = Objects.requireNonNull(config, "config");
    this.driver = Objects.requireNonNull(cleanerDriver, "cleanerDriver");
    this.progressObserver = Objects.requireNonNull(progressObserver, "progressObserver");
    this.primaryKeys = List.copyOf(Objects.requireNonNull(primaryKeys,
        "primaryKeys"));
  }

  /**
   * Processes batches until no matching keys remain.
   *
   * @return number of rows processed or previewed
   * @throws DataException when fetching, deleting, or sleeping fails
   */
  public int runTask() throws DataException {
    int amount = 0;
    int errors = 0;
    boolean processing = true;

    while (processing) {
      List<KeyRow> keys = driver.getRecords(config.getTable(), primaryKeys, config.getWhere(), config.getLimit());
      if (keys.isEmpty()) {
        logProcessedRows(amount);
        processing = false;
        continue;
      }

      if (config.isDryRun()) {
        return previewBatch(amount, keys);
      }

      int updates = driver.deleteRecords(config.getTable(), primaryKeys, config.getWhere(), keys,
          config.shouldCommit());
      if (updates > 0) {
        amount += updates;
        errors = 0;
        updateProgress(amount);
      } else {
        Logger.getGlobal().severe("Update failed with an error!");
        errors++;
        if (errors >= ProcessorConstants.MAX_TASK_ERRORS) {
          throw new DataException(
              String.format("Execution completed due to repeated update errors. Total rows removed %d", amount));
        }
      }

      sleep();
    }
    return amount;
  }

  private int previewBatch(int amount, List<KeyRow> keys) {
    int previewedRows = amount + keys.size();
    updateProgress(previewedRows);
    Logger.getGlobal().info(() ->
        String.format("Dry run completed. Rows that would be removed in the first batch: %d", previewedRows)
    );
    return previewedRows;
  }

  private void logProcessedRows(int amount) {
    Logger.getGlobal().info(() ->
        String.format("Total rows processed: %d", amount)
    );
  }

  private void updateProgress(int amount) {
    progressObserver.setProcessedRows(amount);
  }

  private void sleep() throws DataException {
    try {
      Thread.sleep(config.getSleep());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DataException(String.format("Execution was interrupted with a message: %s", e.getMessage()), e);
    }
  }
}
