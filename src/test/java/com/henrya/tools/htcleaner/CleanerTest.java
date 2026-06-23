package com.henrya.tools.htcleaner;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import picocli.CommandLine;

import static org.assertj.core.api.Assertions.assertThat;
import static com.github.stefanbirkner.systemlambda.SystemLambda.tapSystemErr;
import static com.github.stefanbirkner.systemlambda.SystemLambda.tapSystemOutNormalized;

class CleanerTest {


  @Test
  @DisplayName("Test getters and setters")
  void getterSetterTest() {
    Cleaner cleaner = new Cleaner();
    cleaner.setQuiet(true);
    cleaner.setCountRows(true);
    cleaner.setDatabase("h2");
    cleaner.setDriver("mysql");
    cleaner.setHost("localhost");
    cleaner.setLimit(1000);
    cleaner.setPassword("abc");
    cleaner.setPort(3306);
    cleaner.setDryRun(false);
    cleaner.setPrimaryKey("ID");
    cleaner.setWhere("WHERE");
    cleaner.setProgressDelay(100);
    cleaner.setAskPass(true);
    assertThat(cleaner.isNotQuiet()).isFalse();
    assertThat(cleaner.isCountRows()).isTrue();
    assertThat(cleaner.getDatabase()).isEqualTo("h2");
    assertThat(cleaner.getDriver()).isEqualTo("mysql");
    assertThat(cleaner.getHost()).isEqualTo("localhost");
    assertThat(cleaner.getLimit()).isEqualTo(1000);
    assertThat(cleaner.getPassword()).isEqualTo("abc");
    assertThat(cleaner.getPort()).isEqualTo(3306);
    assertThat(cleaner.isDryRun()).isFalse();
    assertThat(cleaner.shouldCommit()).isTrue();
    assertThat(cleaner.getPrimaryKey()).isEqualTo("ID");
    assertThat(cleaner.getWhere()).isEqualTo("WHERE");
    assertThat(cleaner.getProgressDelay()).isEqualTo(100);
    assertThat(cleaner.isAskPass()).isTrue();
  }

  @Test
  @DisplayName("Test required parameters missing exception")
  void requiredParameterMissingTest() throws Exception {
    String systemError = tapSystemErr(() -> {
      String output = tapSystemOutNormalized(() -> new CommandLine(new Cleaner()).execute());
      assertThat(output).isEmpty();
    });

    assertThat(systemError)
        .contains("Missing required options")
        .contains("'--host=<host>'")
        .contains("'--user=<user>'")
        .contains("'--port=<port>'")
        .contains("'--database=<database>'")
        .contains("'--table=<table>'")
        .contains("'--where=<where>'")
        .contains("Usage: htcleaner")
        .contains("-u")
        .contains("--user=<user>");
  }

  @Test
  @DisplayName("Test required parameters valid")
  void requiredParameterValidTest() throws Exception {
    String systemError = tapSystemErr(() -> {
      String outText = tapSystemOutNormalized(() -> {
        Cleaner cleaner = Mockito.spy(Cleaner.class);
        Mockito.doReturn(CommandLine.ExitCode.OK).when(cleaner).call();
        new CommandLine(cleaner).execute("--user=test", "--password=123", "--host=localhost", "--port=3301",
            "--database=test", "--table=demo", "--where=1=1", "--quiet=false");
      });
      assertThat(outText).isEmpty();
    });
    assertThat(systemError).isEmpty();
  }

  @Test
  @DisplayName("Test validation errors return usage exit code")
  void validationErrorExitCodeTest() throws Exception {
    String systemError = tapSystemErr(() -> {
      int exitCode = new CommandLine(new Cleaner()).execute("--user=test", "--password=123",
          "--host=localhost", "--port=3301", "--database=test", "--table=demo", "--where=1=1", "--limit=0");
      assertThat(exitCode).isEqualTo(CommandLine.ExitCode.USAGE);
    });

    assertThat(systemError)
        .contains("Invalid value '0' for option '--limit':")
        .contains("Usage: htcleaner");
  }

  @Test
  @DisplayName("Invalid where predicate returns usage exit code")
  void invalidWhereExitCodeTest() throws Exception {
    String systemError = tapSystemErr(() -> {
      int exitCode = new CommandLine(new Cleaner()).execute("--user=test", "--password=123",
          "--host=localhost", "--port=3301", "--database=test", "--table=demo", "--where=WHERE id = 1");
      assertThat(exitCode).isEqualTo(CommandLine.ExitCode.USAGE);
    });

    assertThat(systemError)
        .contains("WHERE predicate must not include the WHERE keyword")
        .contains("Usage: htcleaner");
  }

  @Test
  @DisplayName("Runtime failures return software exit code")
  void runtimeFailureExitCodeTest() {
    int exitCode = new CommandLine(new Cleaner()).execute("--user=test", "--password=123",
        "--host=localhost", "--port=3301", "--database=test", "--table=demo", "--where=1=1", "--driver=random");

    assertThat(exitCode).isEqualTo(CommandLine.ExitCode.SOFTWARE);
  }

  @Test
  @DisplayName("Password can be omitted")
  void passwordCanBeOmittedTest() {
    int exitCode = new CommandLine(new Cleaner()).execute("--user=test",
        "--host=localhost", "--port=3301", "--database=test", "--table=demo", "--where=1=1", "--driver=random");

    assertThat(exitCode).isEqualTo(CommandLine.ExitCode.SOFTWARE);
  }

  @Test
  @DisplayName("Ask pass reads password before execution")
  void askPassReadsPasswordTest() {
    Cleaner cleaner = new Cleaner();
    cleaner.setPasswordReader(prompt -> "secret".toCharArray());

    int exitCode = new CommandLine(cleaner).execute("--user=test",
        "--host=localhost", "--port=3301", "--database=test", "--table=demo", "--where=1=1", "--driver=random",
        "--ask-pass");

    assertThat(exitCode).isEqualTo(CommandLine.ExitCode.SOFTWARE);
    assertThat(cleaner.getPassword()).isEqualTo("secret");
  }

  @Test
  @DisplayName("Ask pass without console returns usage exit code")
  void askPassWithoutConsoleExitCodeTest() throws Exception {
    Cleaner cleaner = new Cleaner();
    cleaner.setPasswordReader(prompt -> null);

    String systemError = tapSystemErr(() -> {
      int exitCode = new CommandLine(cleaner).execute("--user=test",
          "--host=localhost", "--port=3301", "--database=test", "--table=demo", "--where=1=1", "--ask-pass");
      assertThat(exitCode).isEqualTo(CommandLine.ExitCode.USAGE);
    });

    assertThat(systemError)
        .contains("Cannot read password from console for --ask-pass")
        .contains("Usage: htcleaner");
  }

  @Test
  @DisplayName("Explicit password wins over ask pass")
  void explicitPasswordWinsOverAskPassTest() {
    Cleaner cleaner = new Cleaner();
    cleaner.setPasswordReader(prompt -> {
      throw new AssertionError("Password prompt should not run when --password is supplied");
    });

    int exitCode = new CommandLine(cleaner).execute("--user=test", "--password=123",
        "--host=localhost", "--port=3301", "--database=test", "--table=demo", "--where=1=1", "--driver=random",
        "--ask-pass");

    assertThat(exitCode).isEqualTo(CommandLine.ExitCode.SOFTWARE);
    assertThat(cleaner.getPassword()).isEqualTo("123");
  }

  @Test
  @DisplayName("Help option follows Percona-style long option")
  void helpOptionTest() throws Exception {
    String output = tapSystemOutNormalized(() -> {
      int exitCode = new CommandLine(new Cleaner()).execute("--help");
      assertThat(exitCode).isEqualTo(CommandLine.ExitCode.OK);
    });

    assertThat(output)
        .contains("Usage: htcleaner")
        .contains("--help")
        .contains("--where=<where>");
  }

  @Test
  @DisplayName("Version option follows Percona-style long option")
  void versionOptionTest() throws Exception {
    String output = tapSystemOutNormalized(() -> {
      int exitCode = new CommandLine(new Cleaner()).execute("--version");
      assertThat(exitCode).isEqualTo(CommandLine.ExitCode.OK);
    });

    assertThat(output).contains("htcleaner ");
  }
}
