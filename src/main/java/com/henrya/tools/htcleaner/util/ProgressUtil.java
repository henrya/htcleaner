package com.henrya.tools.htcleaner.util;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Utility class to display the current progress of the execution
 */
public class ProgressUtil {

    /**
     * Currently processed rows
     */
    private static final AtomicInteger processedRows = new AtomicInteger(0);
    /**
     * Total rows to be processed
     */
    private int totalRows = 0;

    /**
     * Will start a background thread to show the progress of the execution
     *
     * @param progressDelay the internal when progress is displayed
     */
    public Timer displayProgress(int progressDelay) {
        Timer timer = null;
        Logger.getGlobal().info(() -> String.format("Total rows found: %d", totalRows));
        // start timer only when there are rows to process
        if (totalRows > 0) {
            timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    float progress = processedRows.get();
                    // calculate the progress of the execution
                    float percentage = progress / totalRows * 100;
                    Logger.getGlobal().info(() ->
                            String.format("Total progress: %.2f%% [%.0f of %d]", percentage, progress, totalRows)
                    );
                }
            }, progressDelay, progressDelay);
        }
        return timer;
    }

    /**
     * Will set total rows
     *
     * @param totalRows total rows to be set
     */
    public void setTotalRows(int totalRows) {
        this.totalRows = totalRows;
    }

    /**
     * Sets the current amount of rows being processed
     *
     * @param processedRows currently processed rows
     */
    public static void setProcessedRows(int processedRows) {
        ProgressUtil.processedRows.set(processedRows);
    }
}
