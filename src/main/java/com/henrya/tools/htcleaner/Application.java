package com.henrya.tools.htcleaner;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import picocli.CommandLine;

/**
 * Application entrypoint.
 */
public final class Application {
  private static final String LOGGING_PROPERTIES = "logging.properties";

  private Application() {
    throw new UnsupportedOperationException("This class cannot be initialized directly");
  }

  /**
   * CLI entrypoint that leaves JVM termination decisions to the caller.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    run(args);
  }

  /**
   * Runs the CLI command and returns the exit code.
   *
   * @param args command line arguments
   * @return picocli exit code
   */
  public static int run(String[] args) {
    return configureLogging() ? Cleaner.execute(args) : CommandLine.ExitCode.SOFTWARE;
  }

  private static boolean configureLogging() {
    try (InputStream input = Application.class.getClassLoader().getResourceAsStream(LOGGING_PROPERTIES)) {
      if (input == null) {
        Logger.getGlobal().log(Level.SEVERE, "Cannot load logging configuration: resource not found");
        return false;
      }
      LogManager.getLogManager().readConfiguration(input);
      return true;
    } catch (IOException | SecurityException e) {
      Logger.getGlobal().log(Level.SEVERE, "Cannot load logging configuration", e);
      return false;
    }
  }
}
