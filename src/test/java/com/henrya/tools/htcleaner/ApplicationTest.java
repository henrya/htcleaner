package com.henrya.tools.htcleaner;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import static com.github.stefanbirkner.systemlambda.SystemLambda.tapSystemErr;
import static org.assertj.core.api.Assertions.assertThat;

class ApplicationTest {

  @Test
  @DisplayName("Application run returns usage exit code")
  void runReturnsUsageExitCode() throws Exception {
    String systemError = tapSystemErr(() -> {
      int exitCode = Application.run(new String[]{});
      assertThat(exitCode).isEqualTo(CommandLine.ExitCode.USAGE);
    });

    assertThat(systemError).contains("Missing required options");
  }

  @Test
  @DisplayName("Application main does not terminate the JVM")
  void mainDoesNotTerminateJvm() throws Exception {
    String systemError = tapSystemErr(() -> Application.main(new String[]{}));

    assertThat(systemError).contains("Missing required options");
  }
}
