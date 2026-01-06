package com.henrya.tools.htcleaner.dialects;

import com.henrya.tools.htcleaner.exception.CleanerException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DialectFactoryTest {

  @Test
  @DisplayName("Test that MySqlDialect is returned")
  void testMySqlDialect() throws CleanerException {
    DatabaseDialect dialect = DialectFactory.createDialect("mysql");
    assertThat(dialect).isInstanceOf(MySqlDialect.class);
  }

  @Test
  @DisplayName("Test that H2Dialect is returned")
  void testH2Dialect() throws CleanerException {
    DatabaseDialect dialect = DialectFactory.createDialect("h2");
    assertThat(dialect).isInstanceOf(H2Dialect.class);
  }

  @Test
  @DisplayName("Test that OracleDialect is returned")
  void testOracleDialect() throws CleanerException {
    DatabaseDialect dialect = DialectFactory.createDialect("oracle");
    assertThat(dialect).isInstanceOf(OracleDialect.class);
  }

  @Test
  @DisplayName("Test that PostgresDialect is returned")
  void testPostgresDialect() throws CleanerException {
    DatabaseDialect dialect = DialectFactory.createDialect("postgres");
    assertThat(dialect).isInstanceOf(PostgresDialect.class);
  }

  @Test
  @DisplayName("Test that an exception is thrown for an unexpected driver")
  void testUnexpectedDriver() {
    assertThatThrownBy(() -> DialectFactory.createDialect("unknown"))
        .isInstanceOf(CleanerException.class)
        .hasMessage("Unexpected driver: unknown");
  }

  @Test
  @DisplayName("Test that the class cannot be initialized")
  void testInitialization() {
    assertThatThrownBy(DialectFactory::new)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("This class cannot be initialized directly");
  }
}
