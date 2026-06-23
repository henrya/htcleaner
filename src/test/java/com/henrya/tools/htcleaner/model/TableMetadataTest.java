package com.henrya.tools.htcleaner.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableMetadataTest {

  @Test
  @DisplayName("Qualified name prefers schema over catalog")
  void qualifiedNamePrefersSchemaOverCatalog() {
    TableMetadata metadata = new TableMetadata("orders", "public", "sales", "TABLE");

    assertThat(metadata.qualifiedName()).isEqualTo("public.orders");
  }

  @Test
  @DisplayName("Qualified name falls back to catalog")
  void qualifiedNameFallsBackToCatalog() {
    TableMetadata metadata = new TableMetadata("orders", null, "sales", "TABLE");

    assertThat(metadata.qualifiedName()).isEqualTo("sales.orders");
  }

  @Test
  @DisplayName("Table metadata rejects blank table name")
  void rejectsBlankTableName() {
    assertThatThrownBy(() -> new TableMetadata(" ", null, null, "TABLE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Table name cannot be empty");
  }
}
