package com.henrya.tools.htcleaner.tools;

import com.henrya.tools.htcleaner.Cleaner;
import com.henrya.tools.htcleaner.constants.DefaultsConstants;

public class TestConfig {

  public static final String TABLE_NAME = "testtable";

  public static Cleaner getCleaner(){
    Cleaner cleaner = new Cleaner();
    cleaner.setDriver("h2");
    cleaner.setHost("");
    cleaner.setPort(20);
    cleaner.setDatabase("");
    cleaner.setTable(TABLE_NAME);
    cleaner.setUser("sa");
    cleaner.setPassword("");
    cleaner.setCountRows(true);
    cleaner.setLimit(Integer.valueOf(DefaultsConstants.DEFAULT_FETCH_LIMIT));
    cleaner.setSleep(Integer.valueOf(DefaultsConstants.DEFAULT_FETCH_SLEEP_MS));
    cleaner.setQuiet(false);
    cleaner.setDryRun(false);
    cleaner.setProgressDelay(Integer.valueOf(DefaultsConstants.DEFAULT_FETCH_SLEEP_MS));
    return cleaner;
  }
}
