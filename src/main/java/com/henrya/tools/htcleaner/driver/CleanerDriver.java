package com.henrya.tools.htcleaner.driver;

import com.henrya.tools.htcleaner.exception.DataException;

import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

public interface CleanerDriver {
  void connect(String host, Integer port, String database, String user, String password) throws DataException;
  List<String> getPrimaryKeys(@Nonnull String table) throws DataException;
  Map<String,String> getTable(@Nonnull String table) throws DataException;
  List<String> getRecords(@Nonnull String table, @Nonnull  String primaryKey, String where, int limit) throws DataException;
  int deleteRecords(@Nonnull String table, @Nonnull String primaryKey, String where, List<String> keys, boolean commit) throws DataException;
  int countRows(@Nonnull String table, String where) throws DataException;
}
