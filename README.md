# htcleaner

*htcleaner* is a tool designed to remove data from large tables in the database.
Production systems are often sensitive and running `TRUNCATE` or large `DELETE` commands in such systems may lead to replication delays, overload and so on.

It is inspired by pt-archiver, a similar tool that is part of [Percona toolkit](http://www.percona.com/).  

*htcleaner* is using minimum amount of dependencies and only relying on [picocli](https://picocli.info/) and JDBC drivers.

# Command line options

## Required parameters

Database host name, user name, password, database/schema and table name must be specified as required parameters

**--host** or -**h** specifies a database host name

~~~
--host=value
~~~

**--user** or **-u** specifies a database user name

~~~
--user=value
~~~

**--password** or **-p** specifies a password associated with the user name

~~~
--password=value
~~~

**--port** or **-P** specifies a database port name

~~~
--port=value
~~~

**--database** or **-d** specifies a database/schema name

~~~
--database=value
~~~

**--table** or **-t** specifies a table name

~~~
--table=value
~~~

## Optional parameters

**--limit** or **-l** specifies chunk size for each transaction. Default: *1000*.

~~~
--limit=1000
~~~

**--sleep** or **-s** specifies the delay to wait before continuing with the next chunk. Default: *1000* ms.

~~~
--sleep=1000
~~~

**--count-rows** specifies whether to count all the rows before the execution or not. Default: *1*.

~~~
--count-rows=1
~~~

**--dry-run** specifies whether execution should be completed, but the data should not be committed. Default: *0*.

~~~
--dry-run=value
~~~

**--quiet** or **-q** specifies whether to disable unnecessary output / verbose mode Default: *0*

~~~
--quiet=value
~~~

**--progress-delay** specifies how to often to display execution progress. Note that when `--quiet` is 1, progress is not displayed.
Progress related information is also limited when `--count-rows` is 0. Default: *10000* ms

~~~
--progress-delay=10000
~~~

**--driver** Specifies a JDBC driver used for the execution. Default: *mysql*

~~~
--driver value
~~~

**--primary-key** specifies the primary key used for execution. By default, *htcleaner* is trying to detect primary key from the schema.

~~~
--primary-key value
~~~

**--where** or **-w** specifies a `WHERE` statement if some of the rows should not be removed.
However, *htcleaner* does not check for any indexes and it is highly recommended to check whether the statement is hitting the right indexes.

~~~
--where value
~~~

### Examples

Execution without optional parameters. Defaults will be used instead.

~~~
java -jar htcleaner-mysql-1.0.5-jar-with-dependencies.jar --host=localhost --user=username --password=pass --port=3306 --database=my_schema --table=my_table
~~~

Execution with additional parameters and optional `WHERE` statement

~~~
java -jar htcleaner-mysql-1.0.5-jar-with-dependencies.jar --host=localhost --user=username --password=pass --port=3306 --database=my_schema --table=my_table --limit=1000 --sleep=10000 --WHERE="AND user_id > 100"
~~~

Execution without any parameters will display all the parameters, including shorthands

~~~
java -jar htcleaner-mysql-1.0.5-jar-with-dependencies.jar
~~~

# Requirements

* Requires JDK 8. Compatible with JDK 11+.

# Supported JDBC drivers

As of version 1.0.0 only MySQL, support for PostgreSQL and Oracle CE is planned

# Building

* Maven

~~~
mvn verify assembly:single
~~~

* Gradle

~~~
./gradlew build
~~~