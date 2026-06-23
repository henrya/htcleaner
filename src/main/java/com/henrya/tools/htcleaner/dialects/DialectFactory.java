package com.henrya.tools.htcleaner.dialects;

import com.henrya.tools.htcleaner.enums.DriverEnum;
import com.henrya.tools.htcleaner.exception.CleanerException;

import javax.annotation.Nonnull;

public final class DialectFactory {

  DialectFactory() {
    throw new UnsupportedOperationException("This class cannot be initialized directly");
  }

  public static DatabaseDialect createDialect(@Nonnull String driver) throws CleanerException {
    DriverEnum driverEnum = DriverEnum.fromDriverName(driver)
        .orElseThrow(() -> new CleanerException("Unexpected driver: " + driver));

    switch (driverEnum) {
      case MYSQL:
        return new MySqlDialect();
      case H2:
        return new H2Dialect();
      case ORACLE:
        return new OracleDialect();
      case POSTGRES:
        return new PostgresDialect();
      default:
        throw new CleanerException("Unexpected driver: " + driver);
    }
  }
}
