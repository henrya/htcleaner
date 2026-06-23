package com.henrya.tools.htcleaner.enums;

import java.util.Optional;

/**
 * Supported JDBC driver identifiers accepted by the CLI.
 */
public enum DriverEnum {
  MYSQL("mysql"),
  H2("h2"),
  ORACLE("oracle"),
  POSTGRES("postgres");

  private final String driverName;

  DriverEnum(String driverName) {
    this.driverName = driverName;
  }

  /**
   * Resolves a CLI driver identifier to a supported driver enum.
   *
   * @param driverName CLI driver identifier
   * @return matching driver, or empty when unsupported
   */
  public static Optional<DriverEnum> fromDriverName(String driverName) {
    for (DriverEnum driver : values()) {
      if (driver.driverName.equals(driverName)) {
        return Optional.of(driver);
      }
    }
    return Optional.empty();
  }
}
