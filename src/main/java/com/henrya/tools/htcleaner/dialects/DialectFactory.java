package com.henrya.tools.htcleaner.dialects;

import com.henrya.tools.htcleaner.exception.CleanerException;

import javax.annotation.Nonnull;

public class DialectFactory {

    public static DatabaseDialect createDialect(@Nonnull String driver) throws CleanerException {
        switch (driver) {
            case "mysql":
                return new MySqlDialect();
            case "h2":
                return new H2Dialect();
            case "oracle":
                return new OracleDialect();
            case "postgres":
                return new PostgresDialect();
            default:
                throw new CleanerException("Unexpected driver: " + driver);
        }
    }
}
