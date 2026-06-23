package com.henrya.tools.htcleaner.sql;

import com.henrya.tools.htcleaner.exception.DataException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SqlIdentifierTest {

  @Test
  @DisplayName("Quote qualified identifiers")
  void quoteQualifiedIdentifier() throws DataException {
    assertThat(SqlIdentifier.quoteQualified("schema.table_name", "\"", false))
        .isEqualTo("\"schema\".\"table_name\"");
    assertThat(SqlIdentifier.quoteQualified("table_name", "`", true))
        .isEqualTo("`TABLE_NAME`");
  }

  @Test
  @DisplayName("Reject unsafe identifiers")
  void rejectUnsafeIdentifier() {
    assertThatThrownBy(() -> SqlIdentifier.quoteQualified("table;delete", "\"", false))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("Invalid SQL identifier");
  }

  @Test
  @DisplayName("Normalize simple metadata identifiers")
  void normalizeSimpleIdentifier() throws DataException {
    assertThat(SqlIdentifier.normalizeSimple("schema.table_name", true)).isEqualTo("TABLE_NAME");
  }

  @Test
  @DisplayName("Extract metadata qualifier")
  void extractMetadataQualifier() throws DataException {
    assertThat(SqlIdentifier.qualifier("schema.table_name", true)).isEqualTo("SCHEMA");
    assertThat(SqlIdentifier.qualifier("table_name", true)).isNull();
  }

  @Test
  @DisplayName("Constructor cannot be called")
  void constructorCannotBeCalled() throws Exception {
    Constructor<SqlIdentifier> constructor = SqlIdentifier.class.getDeclaredConstructor();
    constructor.setAccessible(true);

    assertThatThrownBy(constructor::newInstance)
        .hasRootCauseInstanceOf(UnsupportedOperationException.class)
        .hasRootCauseMessage("This class cannot be initialized directly");
  }
}
