# htcleaner

`htcleaner` purges rows from large database tables in small batches.
It is intended for production systems where a single large `DELETE` or `TRUNCATE`
could cause replication lag, lock pressure, or general overload.

The tool is inspired by `pt-archiver`, which is part of
[Percona Toolkit](https://www.percona.com/software/database-tools/percona-toolkit).
Direct runtime dependencies are intentionally limited to [picocli](https://picocli.info/),
JDBC drivers, and small supporting annotations.

Like Percona Toolkit tools, `htcleaner` is explicit about destructive work:
`--where` is required. To purge every row in the table, pass `--where=1=1`.

Processing is live-set based: each batch selects the next matching primary keys at
execution time. Rows inserted or updated to match `--where` while a run is active
can be processed by the same run. Progress reporting runs independently of batch
processing when progress output is enabled.

Unqualified table names are resolved in the current namespace reported by the
JDBC connection: current database/catalog for MySQL, and current schema for
PostgreSQL and Oracle. Use `database.table` for MySQL, or `schema.table` for
PostgreSQL and Oracle, when the target is not in the current namespace.

## Command Line Options

### Required Options

- `--host`, `-h`: database host name.
- `--user`, `-u`: database user name.
- `--port`, `-P`: database port number. Must be between `1` and `65535`.
- `--database`, `-d`: database name for MySQL and PostgreSQL, or Oracle service
  name. PostgreSQL and Oracle schemas are selected through `--table`.
- `--table`, `-t`: table name. May be qualified as `database.table` for MySQL
  or `schema.table` for PostgreSQL and Oracle.
- `--where`, `-w`: SQL predicate for rows that should be purged. Do not include
  the `WHERE` keyword. Use `--where=1=1` only when you intentionally want to
  purge every row in the table. The predicate must not include statement
  separators, SQL comments, control characters, or statement-level SQL keywords
  such as `SELECT`, `UPDATE`, `DELETE`, `UNION`, `DROP`, or `TRUNCATE`.

If the database user requires a password, supply it with `--password`/`-p` or use
`--ask-pass` to enter it interactively.

### Optional Options

- `--password`, `-p`: database password. The value is masked in argument logging,
  but can still be captured by shell history or process-list tooling. Prefer
  `--ask-pass` for interactive use.
- `--ask-pass`: prompt for a database password before connecting. An explicit
  `--password` value takes precedence.
- `--limit`, `-l`: batch size for each fetch and delete cycle. Default: `1000`.
  Must be positive. For Oracle single-column primary-key deletes, the effective
  maximum is `1000` because Oracle limits `IN` lists to 1000 expressions.
- `--sleep`, `-s`: delay between batches in milliseconds. Default: `1000`. Must be
  positive.
- `--count-rows`: count matching rows before execution so percentage progress can
  be reported. Default: `true`. Use `--count-rows=false` to skip the count query.
- `--dry-run`: preview the first matching batch without deleting rows. Default:
  `false`.
- `--quiet`, `-q`: disable argument and progress output. Default: `false`.
- `--progress-delay`: progress reporting interval in milliseconds. Default:
  `10000`. Must be positive. Progress is disabled when `--quiet=true`. Percentage
  progress also requires `--count-rows=true`.
- `--driver`: JDBC dialect to use. Default: `mysql`. Release archives bundle
  `mysql`, `postgres`, and `oracle`. The `h2` dialect exists for tests and local
  development, but the H2 JDBC driver is not bundled in the release jar.
- `--primary-key`: override the detected primary key for single-column primary-key
  tables. By default, `htcleaner` detects primary keys from table metadata.
  Composite primary keys are detected and used automatically, and cannot be
  overridden by this option.
- `--help`: show usage and available options, then exit.
- `--version`: show version information from the jar manifest, then exit.

`htcleaner` does not check whether `--where` can use an index. Verify execution
plans before running against large production tables. For destructive runs,
prefer qualified table names when the database user can access multiple
schemas or catalogs with the same table name.

## Examples

Purge all rows with required options and default batch settings:

```bash
java -jar htcleaner-1.1.0-jar-with-dependencies.jar \
  --host=localhost \
  --user=username \
  --ask-pass \
  --port=3306 \
  --database=my_schema \
  --table=my_table \
  --where=1=1
```

Run with a custom batch size, sleep interval, and predicate:

```bash
java -jar htcleaner-1.1.0-jar-with-dependencies.jar \
  --host=localhost \
  --user=username \
  --ask-pass \
  --port=3306 \
  --database=my_schema \
  --table=my_table \
  --limit=1000 \
  --sleep=10000 \
  --where="user_id > 100"
```

Use an explicit password only when non-interactive execution requires it:

```bash
java -jar htcleaner-1.1.0-jar-with-dependencies.jar \
  --host=localhost \
  --user=username \
  --password=pass \
  --port=3306 \
  --database=my_schema \
  --table=my_table \
  --where=1=1
```

Preview the first matching batch without deleting rows:

```bash
java -jar htcleaner-1.1.0-jar-with-dependencies.jar \
  --host=localhost \
  --user=username \
  --ask-pass \
  --port=3306 \
  --database=my_schema \
  --table=my_table \
  --where=1=1 \
  --dry-run
```

Show usage and available options:

```bash
java -jar htcleaner-1.1.0-jar-with-dependencies.jar --help
```

Show version information from the jar manifest:

```bash
java -jar htcleaner-1.1.0-jar-with-dependencies.jar --version
```

## Requirements

`htcleaner` requires Java 11 or newer.

## Supported JDBC Drivers

Version `1.1.0` release archives bundle these driver values:

| Driver value | Database | Table scope |
| --- | --- | --- |
| `mysql` | MySQL | Regular MySQL tables, including partitioned tables |
| `postgres` | PostgreSQL | Regular tables and partitioned parent tables |
| `oracle` | Oracle Database | Regular tables |

The code also includes an `h2` dialect for tests and local development. The H2
JDBC driver is not bundled in the release jar.

## Testing Coverage

The automated test suite uses unit tests, mocked JDBC metadata, and H2-backed
integration coverage. It does not start database containers. End-to-end coverage
against MySQL, PostgreSQL, and Oracle requires externally managed database
instances that match the versions used in your environment.

## Building

Run the full Maven verification, including tests, coverage, SpotBugs, and release
archive creation:

```bash
mvn verify
```

Run the Gradle quality gate:

```bash
./gradlew check
```

## Release Archives

GitHub releases publish a compressed archive named `htcleaner-<version>.tgz`.
The release archive is built by Maven during `mvn verify`.
The archive contains the runnable jar at the archive root:

```text
htcleaner-<version>-jar-with-dependencies.jar
```

For version `1.1.0`, the release page should contain `htcleaner-1.1.0.tgz`.
Its contents should be:

```text
htcleaner-1.1.0-jar-with-dependencies.jar
```
