package com.henrya.tools.htcleaner.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeyRowTest {

  @Test
  @DisplayName("Key row makes defensive copies")
  void keyRowMakesDefensiveCopies() {
    List<String> columns = new ArrayList<>(Collections.singletonList("ID"));
    List<Object> values = new ArrayList<>(Collections.singletonList(1));

    KeyRow keyRow = new KeyRow(columns, values);
    columns.set(0, "OTHER_ID");
    values.set(0, 2);

    assertThat(keyRow.getColumns()).containsExactly("ID");
    assertThat(keyRow.getValues()).containsExactly(1);
    assertThatThrownBy(() -> keyRow.getColumns().add("OTHER_ID"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  @DisplayName("Key row rejects invalid metadata")
  void keyRowRejectsInvalidMetadata() {
    assertThatThrownBy(() -> new KeyRow(Collections.singletonList(" "), Collections.singletonList(1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Column names must be non-empty");
    assertThatThrownBy(() -> new KeyRow(Collections.singletonList("ID"), Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Columns and values must be non-empty and the same size");
  }
}
