package com.henrya.tools.htcleaner.model;

/**
 * JDBC metadata for a resolved table.
 */
public final class TableMetadata {
  private final String name;
  private final String schema;
  private final String catalog;
  private final String type;

  public TableMetadata(String name, String schema, String catalog, String type) {
    if (name == null || name.trim().isEmpty()) {
      throw new IllegalArgumentException("Table name cannot be empty");
    }
    this.name = name;
    this.schema = schema;
    this.catalog = catalog;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public String getSchema() {
    return schema;
  }

  public String getCatalog() {
    return catalog;
  }

  public String getType() {
    return type;
  }

  public String qualifiedName() {
    if (schema != null && !schema.isEmpty()) {
      return schema + "." + name;
    }
    if (catalog != null && !catalog.isEmpty()) {
      return catalog + "." + name;
    }
    return name;
  }
}
