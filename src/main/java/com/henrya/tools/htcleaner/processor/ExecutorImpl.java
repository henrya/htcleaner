package com.henrya.tools.htcleaner.processor;

import com.henrya.tools.htcleaner.Cleaner;
import com.henrya.tools.htcleaner.constants.ProcessorConstants;
import com.henrya.tools.htcleaner.driver.CleanerDriverImpl;
import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.util.ProgressUtil;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The executor implementation class for the processor
 */
public class ExecutorImpl {

    /**
     * Cleaner instance
     */
    private final Cleaner cleaner;
    /**
     * Driver instance
     */
    private final CleanerDriverImpl driver;

    /**
     * Constructor for ExecutorImpl
     *
     * @param cleaner Cleaner
     * @param cleanerDriver CleanerDriverImpl
     */
    public ExecutorImpl(@Nonnull Cleaner cleaner, @Nonnull CleanerDriverImpl cleanerDriver) {
        this.cleaner = cleaner;
        this.driver = cleanerDriver;
    }

    /**
     * Will start the execution.
     * The java.util.Timer may contain a timer to display the progress of the execution
     *
     * @param timer java.util.Timer to track the progress of the execution
     * @return int amount of the rows processed
     */
    public int runTask(Timer timer) {
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

        // amount of the processed rows
        final AtomicInteger amount = new AtomicInteger(0);
        // amount of the errors
        final AtomicInteger errors = new AtomicInteger(0);
        // latch to track the execution
        final CountDownLatch latch = new CountDownLatch(1);

        // start the execution
        executorService.scheduleWithFixedDelay(() -> {
            try {
                List<String> keys = driver.getRecords(cleaner.getTable(), cleaner.getPrimaryKey(), cleaner.getWhere(), cleaner.getLimit());
                // only process where there are any rows found
                if (!keys.isEmpty()) {
                    // delete records and return amount of removed records
                    int updates = driver.deleteRecords(cleaner.getTable(), cleaner.getPrimaryKey(), cleaner.getWhere(), keys, cleaner.isDryRun());
                    if (updates > 0) {
                        amount.getAndAdd(updates);
                        // reset amount of errors
                        errors.getAndSet(0);
                        // set progresss
                        ProgressUtil.setProcessedRows(amount.get());
                    } else {
                        // any other scenario is an error. skip
                        Logger.getGlobal().log(Level.SEVERE, "Update failed with an error!");
                        errors.getAndIncrement();
                    }
                } else {
                    Logger.getGlobal().info(() ->
                            String.format("Total rows removed: %d", amount.get())
                    );
                    endTask(executorService, latch, timer);
                }
            } catch (DataException e) {
                Logger.getGlobal().severe(() ->
                        String.format("Execution failed with an error: %s", e.getMessage())
                );
                if (errors.get() >= ProcessorConstants.MAX_TASK_ERRORS) {
                    Logger.getGlobal().severe(() ->
                            String.format("Execution completed due to errors. Total rows removed %d", amount.get())
                    );
                    endTask(executorService, latch, timer);
                }
                errors.getAndIncrement();
            } catch (Exception e){
              Logger.getGlobal().severe(() ->
                  String.format("Unexpected execution %s. Total rows removed %d", e.getMessage(), amount.get())
              );
              endTask(executorService, latch, timer);
            }
        }, 0, cleaner.getSleep(), TimeUnit.MILLISECONDS);

        try {
            // wait for execution..
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Logger.getGlobal().log(Level.SEVERE, () ->
                    String.format("Execution was interrupted with a message: %s", e.getMessage())
            );
        }
        return amount.get();
    }

    /**
     * Will end the execution , shut down the executor and latch
     *
     * @param executorService java.util.concurrent.ScheduledExecutorService
     * @param latch           java.util.concurrent.CountDownLatch
     * @param timer           java.util.Timer
     */
    private void endTask(@Nonnull ScheduledExecutorService executorService, @Nonnull CountDownLatch latch, Timer timer) {
        if (timer != null) timer.cancel();
        executorService.shutdown();
        // stop the scheduler
        latch.countDown();
    }
}
