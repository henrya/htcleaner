package com.henrya.tools.htcleaner.sql;

import com.henrya.tools.htcleaner.exception.DataException;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Guardrails for CLI-supplied SQL predicates.
 */
public final class SqlPredicate {
  private static final List<String> FORBIDDEN_KEYWORDS = Arrays.asList(
      "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "DROP", "ALTER", "CREATE",
      "TRUNCATE", "GRANT", "REVOKE", "COMMIT", "ROLLBACK", "EXEC", "EXECUTE",
      "CALL", "UNION", "LOAD", "COPY"
  );

  private SqlPredicate() {
    throw new UnsupportedOperationException("This class cannot be initialized directly");
  }

  /**
   * Normalizes a CLI-supplied SQL predicate and rejects statement-shaped input.
   *
   * @param where optional predicate without the {@code WHERE} keyword
   * @return trimmed predicate, or {@code null} when no predicate was supplied
   * @throws DataException when the predicate contains unsafe statement syntax
   */
  public static String normalize(String where) throws DataException {
    if (where == null || where.trim().isEmpty()) {
      return null;
    }

    String predicate = where.trim();
    if (Pattern.compile("^WHERE\\b", Pattern.CASE_INSENSITIVE).matcher(predicate).find()) {
      throw new DataException("WHERE predicate must not include the WHERE keyword");
    }
    if (predicate.indexOf(';') >= 0 || predicate.contains("--") || predicate.contains("/*")
        || predicate.contains("*/")) {
      throw new DataException("WHERE predicate must not contain statement separators or SQL comments");
    }
    for (int index = 0; index < predicate.length(); index++) {
      if (Character.isISOControl(predicate.charAt(index))) {
        throw new DataException("WHERE predicate must not contain control characters");
      }
    }

    String unquoted = removeQuotedLiterals(predicate).toUpperCase(Locale.ROOT);
    for (String keyword : FORBIDDEN_KEYWORDS) {
      if (Pattern.compile("\\b" + keyword + "\\b").matcher(unquoted).find()) {
        throw new DataException("WHERE predicate contains forbidden SQL keyword: " + keyword);
      }
    }
    return predicate;
  }

  private static String removeQuotedLiterals(String value) {
    StringBuilder result = new StringBuilder(value.length());
    boolean inQuote = false;
    int index = 0;
    while (index < value.length()) {
      char current = value.charAt(index);
      if (current == '\'') {
        if (inQuote && index + 1 < value.length() && value.charAt(index + 1) == '\'') {
          result.append("  ");
          index += 2;
        } else {
          inQuote = !inQuote;
          result.append(' ');
          index++;
        }
      } else {
        result.append(inQuote ? ' ' : current);
        index++;
      }
    }
    return result.toString();
  }
}
