package com.henrya.tools.htcleaner.model;

import java.util.List;

/**
 * Primary-key values for one fetched row, preserving primary-key column order.
 */
public final class KeyRow {
  private final List<String> columns;
  private final List<Object> values;

  public KeyRow(List<String> columns, List<Object> values) {
    if (columns == null || values == null || columns.isEmpty() || columns.size() != values.size()) {
      throw new IllegalArgumentException("Columns and values must be non-empty and the same size");
    }
    for (String column : columns) {
      if (column == null || column.trim().isEmpty()) {
        throw new IllegalArgumentException("Column names must be non-empty");
      }
    }
    this.columns = List.copyOf(columns);
    this.values = List.copyOf(values);
  }

  public List<String> getColumns() {
    return columns;
  }

  public List<Object> getValues() {
    return values;
  }
}
