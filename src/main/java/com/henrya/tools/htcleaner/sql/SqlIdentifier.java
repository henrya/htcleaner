package com.henrya.tools.htcleaner.sql;

import com.henrya.tools.htcleaner.exception.DataException;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Validates and quotes simple SQL identifiers controlled by CLI input.
 */
public final class SqlIdentifier {
  private static final String IDENTIFIER = "[A-Za-z_]\\w*";

  private SqlIdentifier() {
    throw new UnsupportedOperationException("This class cannot be initialized directly");
  }

  /**
   * Validates and quotes a simple or schema-qualified SQL identifier.
   *
   * @param identifier identifier to quote
   * @param quote dialect quote character
   * @param upperCase whether to upper-case each identifier part before quoting
   * @return quoted identifier
   * @throws DataException when the identifier contains unsafe characters
   */
  public static String quoteQualified(String identifier, String quote, boolean upperCase) throws DataException {
    List<String> quotedParts = new ArrayList<>();
    for (String part : parts(identifier)) {
      quotedParts.add(quotePart(part, quote, upperCase));
    }
    return String.join(".", quotedParts);
  }

  /**
   * Returns the simple table name portion of a possibly schema-qualified identifier.
   *
   * @param identifier identifier to normalize
   * @param upperCase whether to upper-case the value
   * @return simple identifier name
   * @throws DataException when the identifier contains unsafe characters
   */
  public static String normalizeSimple(String identifier, boolean upperCase) throws DataException {
    List<String> parts = parts(identifier);
    String value = parts.get(parts.size() - 1);
    return upperCase ? value.toUpperCase(Locale.ROOT) : value;
  }

  /**
   * Returns the qualifier portion of an identifier, or {@code null} for simple identifiers.
   *
   * @param identifier identifier to inspect
   * @param upperCase whether to upper-case the qualifier
   * @return qualifier or {@code null}
   * @throws DataException when the identifier contains unsafe characters
   */
  public static String qualifier(String identifier, boolean upperCase) throws DataException {
    List<String> parts = parts(identifier);
    if (parts.size() == 1) {
      return null;
    }
    List<String> qualifierParts = parts.subList(0, parts.size() - 1);
    return qualifierParts.stream()
        .map(part -> upperCase ? part.toUpperCase(Locale.ROOT) : part)
        .collect(Collectors.joining("."));
  }

  private static List<String> parts(String identifier) throws DataException {
    if (identifier == null || identifier.trim().isEmpty()) {
      throw new DataException("Identifier cannot be empty");
    }
    List<String> parts = new ArrayList<>();
    for (String part : identifier.split("\\.")) {
      String trimmed = part.trim();
      validatePart(trimmed);
      parts.add(trimmed);
    }
    return parts;
  }

  private static String quotePart(String part, String quote, boolean upperCase) throws DataException {
    validatePart(part);
    String value = upperCase ? part.toUpperCase(Locale.ROOT) : part;
    return quote + value + quote;
  }

  private static void validatePart(String part) throws DataException {
    if (part == null || !part.matches(IDENTIFIER)) {
      throw new DataException("Invalid SQL identifier: " + part);
    }
  }
}
