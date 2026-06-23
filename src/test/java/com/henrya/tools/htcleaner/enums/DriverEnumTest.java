package com.henrya.tools.htcleaner.enums;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DriverEnumTest {

  @Test
  void supportedDriverNamesCanBeResolved() {
    assertThat(DriverEnum.fromDriverName("mysql")).contains(DriverEnum.MYSQL);
    assertThat(DriverEnum.fromDriverName("h2")).contains(DriverEnum.H2);
    assertThat(DriverEnum.fromDriverName("oracle")).contains(DriverEnum.ORACLE);
    assertThat(DriverEnum.fromDriverName("postgres")).contains(DriverEnum.POSTGRES);
  }

  @Test
  void unsupportedDriverNamesAreRejected() {
    assertThat(DriverEnum.fromDriverName("db2")).isEmpty();
  }
}
