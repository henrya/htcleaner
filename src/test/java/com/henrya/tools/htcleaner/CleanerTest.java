package com.henrya.tools.htcleaner;

import org.junit.jupiter.api.Test;
import org.mockito.*;
import picocli.CommandLine;

import static org.assertj.core.api.Assertions.assertThat;
import static com.github.stefanbirkner.systemlambda.SystemLambda.tapSystemErr;
import static com.github.stefanbirkner.systemlambda.SystemLambda.tapSystemOutNormalized;

class CleanerTest {

  @Test
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
