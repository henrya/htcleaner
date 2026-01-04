package com.henrya.tools.htcleaner.dialects;

import com.henrya.tools.htcleaner.constants.ProcessorConstants;
import com.henrya.tools.htcleaner.exception.DataException;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class OracleDialect implements DatabaseDialect {
    @Override
    public List<String> getPrimaryKeys(@Nonnull Connection conn, @Nonnull String table) throws DataException {
        List<String> pkColumns = new ArrayList<>();
        try {
            ResultSet rs = conn.getMetaData().getPrimaryKeys(null, null, table);
            while (rs.next()) {
                pkColumns.add(rs.getString("COLUMN_NAME"));
            }
        } catch (SQLException e) {
            throw new DataException(String.format("Cannot find primary keys from the table %s: %s", table, e));
        }
        return pkColumns;
    }

    @Override
    public List<String> getRecords(@Nonnull Connection conn, @Nonnull String table, @Nonnull String primaryKey, String where, int limit) throws DataException {
        List<String> records = new ArrayList<>();
        String sql = String.format("SELECT %s FROM %s %s WHERE ROWNUM <= %d",
                primaryKey, table, (where != null ? "WHERE " + where : ""), limit);

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                records.add(rs.getString(primaryKey));
            }
        } catch (SQLException e) {
            throw new DataException(String.format("Cannot get records, table %s: %s", table, e));
        }
        return records;
    }

    @Override
    public String getConnectionURI() {
        return ProcessorConstants.CONN_URI_ORACLE;
    }
}
