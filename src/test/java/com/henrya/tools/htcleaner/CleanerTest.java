package com.henrya.tools.htcleaner;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.*;
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
    assertThat(cleaner.isNotQuiet()).isFalse();
    assertThat(cleaner.isCountRows()).isTrue();
    assertThat(cleaner.getDatabase()).isEqualTo("h2");
    assertThat(cleaner.getDriver()).isEqualTo("mysql");
    assertThat(cleaner.getHost()).isEqualTo("localhost");
    assertThat(cleaner.getLimit()).isEqualTo(1000);
    assertThat(cleaner.getPassword()).isEqualTo("abc");
    assertThat(cleaner.getPort()).isEqualTo(3306);
    assertThat(cleaner.isDryRun()).isTrue();
    assertThat(cleaner.getPrimaryKey()).isEqualTo("ID");
    assertThat(cleaner.getWhere()).isEqualTo("WHERE");
    assertThat(cleaner.getProgressDelay()).isEqualTo(100);
  }

  @Test
  @DisplayName("Test required parameters missing exception")
  void requiredParameterMissingTest() throws Exception{
    String systemError = tapSystemErr(() -> {
      String output = tapSystemOutNormalized(() -> new CommandLine(new Cleaner()).execute());
      assertThat(output).isEmpty();
    });

    assertThat(systemError).isEqualTo("Missing required options: '--host=<host>', '--user=<user>', '--password=<password>', '--port=<port>', '--database=<database>', '--table=<table>'\n" +
            "Usage: htcleaner [-q] [--count-rows] [--dry-run] -d=<database>\n" +
            "                 [--driver=<driver>] -h=<host> [-l=<limit>] -p=<password>\n" +
            "                 -P=<port> [--primary-key=<primaryKey>]\n" +
            "                 [--progress-delay=<progressDelay>] [-s=<sleep>] -t=<table>\n" +
            "                 --u=<user> [-w=<where>]\n" +
            "Clean large tables\n" +
            "      --count-rows         Executes count query to measure progress of the\n" +
            "                             execution\n" +
            "  -d, --database=<database>\n" +
            "                           Database information\n" +
            "      --driver=<driver>    JDBC Database driver\n" +
            "      --dry-run            Performs fetch, but does not execute purge\n" +
            "  -h, --host=<host>        Database host name\n" +
            "  -l, --limit=<limit>      The limit set in each fetch\n" +
            "  -p, --password=<password>\n" +
            "                           Database password\n" +
            "  -P, --port=<port>        Port number\n" +
            "      --primary-key=<primaryKey>\n" +
            "                           Primary key to be used\n" +
            "      --progress-delay=<progressDelay>\n" +
            "                           Interval when progress is updated\n" +
            "  -q, --quiet              Do not print any statistcs\n" +
            "  -s, --sleep=<sleep>      Sleep time between fetches\n" +
            "  -t, --table=<table>      Table information\n" +
            "      --u, --user=<user>   Database user name\n" +
            "  -w, --where=<where>      Where clause when fetching the rows\n");
  }

  @Test
  @DisplayName("Test required parameters valid")
  void requiredParameterValidTest() throws Exception{
    String systemError = tapSystemErr(() -> {
      String outText = tapSystemOutNormalized(() -> {
        Cleaner cleaner = Mockito.spy(Cleaner.class);
        Mockito.doNothing().when(cleaner).run();
        new CommandLine(cleaner).execute("--user=test", "--password=123", "--host=localhost", "--port=3301", "--database=test", "--table=demo",  "--where=1=1", "--quiet=false");
      });
      assertThat(outText).isEmpty();
    });
    assertThat(systemError).isEmpty();
  }
}
