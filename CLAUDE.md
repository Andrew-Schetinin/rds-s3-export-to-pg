# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Go command-line tool for restoring PostgreSQL databases from AWS RDS Parquet exports. The tool reads Parquet files exported from AWS RDS snapshots (either from local disk or S3) and loads the data into a target PostgreSQL database.

**Important Context:**
- The project is in active development and not yet production-ready
- The tool relies on the target database already having a complete schema in place (indexes, constraints, sequences, etc.)
- Data loading order is critical and is managed via topological sorting based on foreign key dependencies
- The tool is NOT a replacement for standard PostgreSQL backup/restore tools

For developer setup, tooling, and git workflow see [DEVELOPMENT.md](DEVELOPMENT.md).

## Commit Format

Every commit must follow this format (enforced by Husky):

```
type(#ticket): description
```

Allowed types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `ci`, `perf`, `style`, `build`, `revert`.

## Build and Test Commands

```bash
cd src && go build                                           # compile
cd src && go test -v ./...                                   # all tests
cd src && go test -v -skip "TestCreateTestDatabase" ./...   # skip DB tests (CI)
cd src && golangci-lint run ./...                            # lint
./testdata/docker_test.sh                                    # schema+seed+validate via Docker (default: PG 16)
./testdata/docker_test.sh 17                                 # same, PostgreSQL 17
```

Go database tests require a local PostgreSQL instance and `src/.test_config.yaml` — see DEVELOPMENT.md.

**PostgreSQL access:** There is no local PostgreSQL server available. Always use Docker for any database operations (running SQL files, manual queries, etc.). The images `postgis/postgis:16-3.4` and `postgis/postgis:17-3.5` are already available locally. Use `docker run` + `docker exec` or `./testdata/docker_test.sh` — never attempt to connect with a bare `psql` command.

## Architecture Overview

### Data Flow

1. **Configuration Loading** (`config/`) - Loads settings from environment variables and command-line arguments
2. **Orchestration** (`main.go`) - Ties all packages together; entry point that drives the restore pipeline
3. **Source Layer** (`source/`) - Abstracts file access (local or S3)
4. **Parquet Reading** - Parses Parquet files and extracts table metadata (via AWS RDS export manifest files in JSON)
5. **Table Ordering** (`dag/`) - Builds dependency graph from foreign keys and performs topological sort
6. **Data Writing** (`target/`) - Connects to PostgreSQL and loads data with proper constraint/trigger management

### Key Packages

**`source/` - Data Source Abstraction**
- `source.go` - Defines the `Source` interface for file access (local or remote)
- `source_local.go` - Implements local filesystem access
- `source_s3.go` - Stub for S3 implementation (not yet complete)
- `source_reader.go` - Reads RDS export manifests; defines `ColumnInfo`, `ParquetFileInfo` structs and `Reader` with `IterateOverTables()`/`ListDatabases()` methods
- `parquet_reader.go` - Implements `pgx.CopyFromSource` interface to stream Parquet data
- `transformer.go` - Defines the `Transformer` interface for converting Parquet values

**`target/` - Database Writing**
- `db_writer.go` - Core interface with connection management and table ordering
- `db_writer_impl.go` - Table data writing with transaction management
- `db_writer_tools.go` - Index/constraint management (drop before loading, restore after)
- `field_mapper.go` - Maps Parquet column types to PostgreSQL types and applies filtering rules
- `sql.go` - SQL query templates

**`dag/` - Foreign Key Dependency Graph**
- `entities_dag.go` - DAG implementation with topological sorting to determine table load order

**`config/` - Configuration Management**
- Singleton pattern with lazy initialization
- Priority order: defaults → environment variables → command-line arguments
- Config file loading is not yet implemented (stub with TODO)
- Table filtering via `--include-tables` and `--exclude-tables`
- Supports incremental loads with `--skip-not-empty`

**`utils/` - Shared Utilities**
- `logger.go` - Structured logging with zap (supports JSON, dev, verbose, trace modes)
- `csv_utils.go` - CSV conversion for PostgreSQL COPY command (handles NULL vs empty string distinction)
- `sql_utils.go` - Table name parsing and sanitization

### Critical Design Patterns

**Foreign Key Ordering:**
- Tables are sorted topologically based on foreign key relationships
- Parent tables (referenced) load before child tables (referencing)
- Prevents referential integrity violations during loading
- Self-references and cycles are detected; cyclic references not yet supported

**Type Mapping Strategy:**
- Column metadata comes from RDS export JSON manifests (ColumnInfo structs)
- Maps PostgreSQL types to Parquet types and back
- Special handling for custom types (USER-DEFINED) and arrays
- Currently supports: numeric, text, timestamp, date, jsonb, booleans, integers, floats, etc.

**Constraint Handling:**
- During data load: constraints are deferred, triggers disabled, indexes dropped
- After data load: indexes restored, triggers re-enabled, constraints checked
- This prevents constraint violations during incremental/staged loading

**Data Loading Methods:**
- Binary format (via `pgx.CopyFrom`) when all columns are supported
- CSV format (via PostgreSQL COPY) when user-defined types are present
- CSV conversion handles PostgreSQL's NULL vs empty string distinction using a placeholder character

### Configuration & Command-Line Arguments

Key flags:
- `--dir` - Local directory with Parquet files
- `--s3-bucket` - S3 bucket path (not yet implemented)
- `--source-db` - Database name within export (if multiple DBs exist)
- `--db-name`, `--db-user`, `--db-password`, `--db-host`, `--db-port` - Target PostgreSQL connection
- `--include-tables` / `--exclude-tables` - Filter which tables to load
- `--ignore-missing-tables` - Ignore missing partitioned tables
- `--skip-not-empty` - Skip tables that already have data
- `--truncate-all` - Clear tables before loading
- `--list` - List available databases and exit
- `--json-logs` / `--verbose` / `--trace` / `--dev-logs` - Logging control

## Known Limitations & Future Work

1. **Partitioned tables** - Not yet supported
2. **PostGIS data types** - Limited support
3. **S3 loading** - Stub only, not implemented
4. **DAG cycles** - Cannot handle cyclic foreign key references

## Project Status

- Version: 0.1 (see `version.yaml`)
- **Not production-ready** - Use with test data only
- Active development with open issues on GitHub
