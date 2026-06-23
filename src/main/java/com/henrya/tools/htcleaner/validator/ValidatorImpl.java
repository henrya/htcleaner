package com.henrya.tools.htcleaner.validator;

import com.henrya.tools.htcleaner.Cleaner;
import com.henrya.tools.htcleaner.constants.ParameterConstants;
import com.henrya.tools.htcleaner.exception.DataException;
import com.henrya.tools.htcleaner.sql.SqlPredicate;
import picocli.CommandLine;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
/**
 * Validates input parameters and displays output of the arguments
 */
public final class ValidatorImpl {

  /**
   * Arguments to be validates as positive numbers
   */
  static final List<String> POSITIVE_NUMBER;
  /**
   * Arguments to be masked
   */
  static final List<String> MASK_VALUE;
  /**
   * Meta-options that should not be logged as execution parameters.
   */
  static final List<String> DISPLAY_ONLY;

  static {
    POSITIVE_NUMBER = Arrays.asList(
        ParameterConstants.PARAMETER_PORT_SHORT,
        ParameterConstants.PARAMETER_PORT_LONG,
        ParameterConstants.PARAMETER_LIMIT_SHORT,
        ParameterConstants.PARAMETER_LIMIT_LONG,
        ParameterConstants.PARAMETER_SLEEP_LONG,
        ParameterConstants.PARAMETER_SLEEP_SHORT,
        ParameterConstants.PARAMETER_PROGRESS_DELAY_LONG
    );

    MASK_VALUE = Arrays.asList(
        ParameterConstants.PARAMETER_PASSWORD_SHORT,
        ParameterConstants.PARAMETER_PASSWORD_LONG
    );

    DISPLAY_ONLY = Arrays.asList(
        ParameterConstants.PARAMETER_HELP_LONG,
        ParameterConstants.PARAMETER_VERSION_LONG
    );
  }

  private ValidatorImpl() {
    throw new UnsupportedOperationException("This class cannot be initialized directly");
  }

  /**
   * Validates arguments
   *
   * @param cleaner Cleaner
   */
  public static List<String> validate(@Nonnull Cleaner cleaner) {
    CommandLine spec = cleaner.getSpec().commandLine();
    List<CommandLine.Model.OptionSpec> options = spec.getCommandSpec().options();
    List<String> arguments = new ArrayList<>();
    for (CommandLine.Model.OptionSpec opt : options) {
      if (DISPLAY_ONLY.contains(opt.longestName())) {
        continue;
      }
      checkArguments(opt.longestName(), opt.getValue(), spec);
      // add only arguments if quiet mode is off
      if (cleaner.isNotQuiet()) {
        arguments.add(processValues(opt.longestName(), opt.getValue()));
      }
    }
    return arguments;
  }

  /**
   * Check argument names and values
   *
   * @param name  String name
   * @param value Object value
   * @param spec  CommandLine spec
   * @throws CommandLine.ParameterException Exception to be thrown
   */
  private static void checkArguments(@Nonnull String name, @Nonnull Object value, CommandLine spec)
      throws CommandLine.ParameterException {
    if (POSITIVE_NUMBER.contains(name) && (int) value <= 0) {
      throw new CommandLine.ParameterException(spec,
          String.format("Invalid value '%s' for option '%s': ", value, name));
    }
    if ((ParameterConstants.PARAMETER_PORT_LONG.equals(name) || ParameterConstants.PARAMETER_PORT_SHORT.equals(name))
        && (int) value > 65535) {
      throw new CommandLine.ParameterException(spec,
          String.format("Invalid value '%s' for option '%s': ", value, name));
    }
    if (ParameterConstants.PARAMETER_WHERE_LONG.equals(name)) {
      try {
        SqlPredicate.normalize((String) value);
      } catch (DataException e) {
        throw new CommandLine.ParameterException(spec,
            String.format("Invalid value '%s' for option '%s': %s", value, name, e.getMessage()));
      }
    }
  }

  /**
   * Process arguments to be displayed when quiet mode is off
   *
   * @param name  String name
   * @param value Object value
   * @return String message
   */
  private static String processValues(@Nonnull String name, @Nonnull Object value) {
    if (MASK_VALUE.contains(name)) {
      return String.format("%s=%s", name, "****");
    } else {
      return String.format("%s=%s (arg value or default)", name, value);
    }
  }
}
