package com.henrya.tools.htcleaner.processor;

import com.henrya.tools.htcleaner.Cleaner;
import com.henrya.tools.htcleaner.driver.CleanerDriverImpl;
import com.henrya.tools.htcleaner.exception.CleanerException;
import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.util.ProgressUtil;

import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

import java.util.Timer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Processor class for the executor
 */
public class ProcessorImpl {

  /**
   * Will process the task with given parameters
   *
   * @param cleaner Cleaner instance
   * @param cleanerDriver Driver instance
   * @throws CleanerException exception to be thrown
   */
  public void process(@Nonnull Cleaner cleaner, @Nonnull CleanerDriverImpl cleanerDriver) throws CleanerException {
    try {
      cleanerDriver.connect(cleaner.getHost(), cleaner.getPort(),
          cleaner.getDatabase(), cleaner.getUser(),
          cleaner.getPassword());
      if (cleanerDriver.getConn() != null) {
        Map<String, String> tableInfo = cleanerDriver.getTable(cleaner.getTable());
        if (!tableInfo.isEmpty()) {
          String primaryKey;
          List<String> primaryKeys = cleanerDriver.getPrimaryKeys(cleaner.getTable());
          // check whether primary key was given as an argument
          if (cleaner.getPrimaryKey() != null && !cleaner.getPrimaryKey().isEmpty()) {
            if (!primaryKeys.contains(cleaner.getPrimaryKey())) {
              throw new CleanerException(
                  String.format("Primary key: %s is not valid", cleaner.getPrimaryKey()));
            } else {
              primaryKey = cleaner.getPrimaryKey();
            }
          } else {
            primaryKey = primaryKeys.stream().findFirst()
                .orElseThrow(() -> new CleanerException("Cannot find primary keys!"));
            cleaner.setPrimaryKey(primaryKey);
          }

          Logger.getGlobal().info(() ->
              String.format("Using primary key: %s", primaryKey)
          );

          processTask(cleanerDriver, cleaner, primaryKey);

        } else {
          Logger.getGlobal().log(Level.SEVERE, () ->
              String.format("Aborting. Table %s does not exist!", cleaner.getTable())
          );
        }
      }
    } catch (DataException e) {
      Logger.getGlobal().log(Level.SEVERE, () ->
          String.format("Aborting. Failed with an exception: %s!", e.getMessage())
      );
    }
  }

  /**
   * Will process the task and returns the result of successful records
   *
   * @param cleanerDriver     CleanerDriverImpl cleanerDriver name
   * @param cleaner    Cleaner instance
   * @param primaryKey String primary key
   */
  private void processTask(@Nonnull CleanerDriverImpl cleanerDriver, @Nonnull Cleaner cleaner,
      @Nonnull String primaryKey) throws DataException {

    int totalRows = 0;
    if (cleaner.isCountRows()) {
      totalRows = cleanerDriver.countRows(cleaner.getTable(), cleaner.getWhere());
    }

    Logger.getGlobal().info(() ->
        String.format("Executing with parameters: table %s, primaryKey: %s, limit: %s",
            cleaner.getTable(), primaryKey, cleaner.getLimit())
    );

    Timer timer = null;
            ProgressUtil progressUtil = null;
            if (cleaner.isNotQuiet()) {
                progressUtil = new ProgressUtil();
                progressUtil.setTotalRows(totalRows);
                timer = progressUtil.displayProgress(cleaner.getProgressDelay());
            }
    
            ExecutorImpl taskExecutor = new ExecutorImpl(cleaner, cleanerDriver, progressUtil);
    int records = taskExecutor.runTask(timer);
    Logger.getGlobal().info(() ->
        String.format("Total records processed: %d", records)
    );
  }
}