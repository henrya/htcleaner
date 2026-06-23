package com.henrya.tools.htcleaner.sql;

import com.henrya.tools.htcleaner.exception.DataException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SqlPredicateTest {

  @Test
  @DisplayName("Normalize valid predicates")
  void normalizeValidPredicate() throws DataException {
    assertThat(SqlPredicate.normalize(" id > 10 AND name = 'select' "))
        .isEqualTo("id > 10 AND name = 'select'");
  }

  @Test
  @DisplayName("Reject statement-shaped predicates")
  void rejectStatementShapedPredicate() {
    assertThatThrownBy(() -> SqlPredicate.normalize("WHERE id > 10"))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("must not include the WHERE keyword");
    assertThatThrownBy(() -> SqlPredicate.normalize("id > 10; DELETE FROM t"))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("statement separators");
    assertThatThrownBy(() -> SqlPredicate.normalize("id IN (SELECT id FROM t)"))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("forbidden SQL keyword");
  }

  @Test
  @DisplayName("Constructor cannot be called")
  void constructorCannotBeCalled() throws Exception {
    Constructor<SqlPredicate> constructor = SqlPredicate.class.getDeclaredConstructor();
    constructor.setAccessible(true);

    assertThatThrownBy(constructor::newInstance)
        .hasRootCauseInstanceOf(UnsupportedOperationException.class)
        .hasRootCauseMessage("This class cannot be initialized directly");
  }
}
