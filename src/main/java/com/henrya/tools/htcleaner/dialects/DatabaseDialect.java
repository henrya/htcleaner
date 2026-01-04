package com.henrya.tools.htcleaner.dialects;

import com.henrya.tools.htcleaner.exception.DataException;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public interface DatabaseDialect {

    List<String> getPrimaryKeys(@Nonnull Connection conn, @Nonnull String table) throws DataException;

    List<String> getRecords(@Nonnull Connection conn, @Nonnull String table, @Nonnull String primaryKey, String where, int limit) throws DataException;

    String getConnectionURI() throws SQLException;
}
