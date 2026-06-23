package com.henrya.tools.htcleaner.tools;

import com.henrya.tools.htcleaner.Cleaner;
import com.henrya.tools.htcleaner.constants.DefaultsConstants;
import com.henrya.tools.htcleaner.model.KeyRow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class TestConfig {

  public static final String TABLE_NAME = "testtable";

  public static Cleaner getCleaner() {
    Cleaner cleaner = new Cleaner();
    cleaner.setDriver("h2");
    cleaner.setHost("");
    cleaner.setPort(20);
    cleaner.setDatabase("");
    cleaner.setTable(TABLE_NAME);
    cleaner.setWhere("1=1");
    cleaner.setUser("sa");
    cleaner.setPassword("");
    cleaner.setCountRows(true);
    cleaner.setLimit(Integer.valueOf(DefaultsConstants.DEFAULT_FETCH_LIMIT));
    cleaner.setSleep(1);
    cleaner.setQuiet(false);
    cleaner.setDryRun(false);
    cleaner.setProgressDelay(100);
    return cleaner;
  }

  public static List<String> primaryKeys() {
    return Collections.singletonList("ID");
  }

  public static List<KeyRow> keyRows(Object... values) {
    List<KeyRow> rows = new ArrayList<>();
    for (Object value : values) {
      rows.add(new KeyRow(primaryKeys(), Collections.singletonList(value)));
    }
    return rows;
  }

  private TestConfig() {
    throw new UnsupportedOperationException("This class cannot be initialized directly");
  }
}
