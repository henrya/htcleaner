package com.henrya.tools.htcleaner.driver;

import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.model.KeyRow;
import com.henrya.tools.htcleaner.model.TableMetadata;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Database operations required by the cleaner processor.
 */
public interface CleanerDriver {
  void connect(String host, Integer port, String database, String user, String password) throws DataException;

  List<String> getPrimaryKeys(@Nonnull String table) throws DataException;

  Optional<TableMetadata> getTable(@Nonnull String table) throws DataException;

  List<KeyRow> getRecords(@Nonnull String table, @Nonnull List<String> primaryKeys, String where, int limit)
      throws DataException;

  int deleteRecords(@Nonnull String table, @Nonnull List<String> primaryKeys, String where, List<KeyRow> keys,
      boolean commit) throws DataException;

  int countRows(@Nonnull String table, String where) throws DataException;

  boolean isConnected();
}
