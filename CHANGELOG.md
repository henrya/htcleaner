# Changelog

**2026-06-23**
1.1.0 - Audit hardening and database support improvements
- Added safer MySQL, PostgreSQL, Oracle, and H2 cleanup behavior with validated SQL identifiers, prepared deletes, composite primary-key support, and partitioned-table metadata coverage.
- Aligned CLI behavior with Percona-style usage: `--where` is required, `--where=1=1` is explicit for all rows, and `--ask-pass`, `--help`, and `--version` are supported.
- Moved version reporting to Maven/Gradle jar manifest metadata and updated release packaging to publish a `.tgz` containing the fat jar.
- Separated progress reporting from batch processing, improved package structure, and removed stale utility/enum-singleton patterns.
- Added CI, tag-based release workflow, SpotBugs, JaCoCo coverage checks, and expanded unit/integration coverage.

**2025-07-04**
1.0.5 - Various dependency upgrades

**2025-04-11**
1.0.4 - Various dependency upgrades

**2024-04-23**
1.0.3 -  Various dependency upgrades and JDK 21 support

**2023-12-19**
1.0.2 -  Various dependency upgrades

**2022-11-05**
1.0.1 -  Various dependency upgrades

**2022-05-31**
1.0.0 -  Initial release
