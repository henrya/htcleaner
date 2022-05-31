package com.henrya.tools.htcleaner;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Main class for the application
 */
public class Application {
  public static void main(String[] args) {
    try {
      // load logging properties
      LogManager.getLogManager().readConfiguration(
          Application.class.getClassLoader().getResourceAsStream("logging.properties"));
      Cleaner.main(args);
    } catch (IOException e) {
      Logger.getGlobal().log(Level.SEVERE, "Cannot load logging configuration!");
    }
  }
}
