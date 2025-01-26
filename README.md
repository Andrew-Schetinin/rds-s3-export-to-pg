# Restore PostgreSQL databases from AWS RDS S3 exports in Parquet format

AWS RDS supports exporting database snapshots to S3 in Parquet format.
This functionality is described here 

* https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ExportSnapshot.html
* https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/postgresql-s3-export.html

This command line tool allows restoring a PostgreSQL database from such an export.

The program expects to have a properly exported AWS RDS snapshot at S3 or in the local file system,
and an empty target database into which the snapshot will be restored (using a schema-only restore via pg_restore.

The target database must exist because Parquet does not contain sufficient information to restore indexes, foreign references, triggers, sequences, constraints, etc.

The data is loaded as efficiently as possible - it is unlikely to be as efficient as pg_restore, but it is expected to be reasonably close.

The program avoids breaking referential integrity by organizing tables in a DAG (directed acyclic graph) and ordering tables, 
using topological sorting, so that they load from referenced to referencing tables.

# 1. Usage

## 1.1. Disclaimer

Use this project at your own risk. 
No guarantees and no obligations whatsoever.
Don't use this project with your production data.

Currently, the project is in active development and is not ready yet for usage.

Planned action items:

1. (a must) [#1](https://github.com/Andrew-Schetinin/rds-s3-export-to-pg/issues/1) - Implement support for partitioned tables (AWS RDS exports ).
2. (a must) [#2](https://github.com/Andrew-Schetinin/rds-s3-export-to-pg/issues/2) - Support additional PostgreSQL data types - for example PostGIS geography.
3. (nice to have) [#3](https://github.com/Andrew-Schetinin/rds-s3-export-to-pg/issues/3) - Implement S3 configurations and loading files from there.
4. (nice to have) [#6](https://github.com/Andrew-Schetinin/rds-s3-export-to-pg/issues/6) - Implement releases' compilation for different platforms
5. (future) [#4](https://github.com/Andrew-Schetinin/rds-s3-export-to-pg/issues/4) - Implement a complete integration test with multiple samples of different data types and PostgreSQL features.
6. (future) [#5](https://github.com/Andrew-Schetinin/rds-s3-export-to-pg/issues/5) - Implement support for DAG cycles (cycled references between PostgreSQL tables)

## 1.2. Supported platforms

These are the planned platforms.
Binaries are not yet built for those platforms.

1. MacOS arm64
2. Ubuntu Linux 22.04 amd64
3. MS Windows amd64

## 1.3. Usage and command line arguments

Run the program with the `--help` argument to receive the list of supported command line arguments.

The program expects to find the RDS export either locally or remotely on S3.

The target database, into which data is loaded, has to exist and contain complete (and compatible) schema.

## 1.4. Frequently asked questions

1. Why developing this tool?
   * Restoring from AWS RDS exports is a frequent question on StackOverflow and in forums, 
   and there is no suitable tool.  
2. Why Go?
   * Go is great for microservices and lambdas because of its resource efficiency and performance. 
   It is also highly suitable for light-weight command line tools that need to run inside Docker like this one.
3. Is this a commercial project?
   * No, it is not, and it is not planned as such - hence the open source license.

# 2. Development

## 2.1. Updating dependencies

When changing anything in dependencies, the following command has to be executed 
to update the `go.mod` and `go.sum` files:

```bash
cd src
go mod tidy
```

Updating all dependencies to new versions:

```bash
cd src
go get -u
```

This is especially important before building the Docker image.
TODO: Ideally, it should be automated as part of the regular build.

## 2.2. Compilation

Before compilation, one must generate that Swagger documentation as described above, 
because it is embedded into the executable.

```bash
cd src
rm -f ./dbrestore # on Linux/MacOS  
go build
```

Removal is useful because any warning or error cause the build to fail creating a new binary, 
and it is easy to miss because the old binary remains.

## 2.3. Running unit tests

Simple `go test` fails, so it has to be run like the following:

```bash
cd src
go test -v ./...
```

Some tests require PostgreSQL to be installed and accessible on localhost with the default `postgres` user
and the default port 5432. 

The password must be specified via the local file 
named `.test_config.yaml` (note the dot in front) inside the `src` folder.

This file should contain a single line with the password:

```yaml
password: POSTGRES_USER_PASSWORD 
```
