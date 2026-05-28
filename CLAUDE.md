# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Go command-line tool for restoring PostgreSQL databases from AWS RDS Parquet exports. The tool reads Parquet files exported from AWS RDS snapshots (either from local disk or S3) and loads the data into a target PostgreSQL database.

**Important Context:**
- The project is in active development and not yet production-ready
- The tool relies on the target database already having a complete schema in place (indexes, constraints, sequences, etc.)
- Data loading order is critical and is managed via topological sorting based on foreign key dependencies
- The tool is NOT a replacement for standard PostgreSQL backup/restore tools

## OpenSpec

This project uses [OpenSpec](https://github.com/fission-ai/openspec) (`@fission-ai/openspec`) for spec-driven development. Specs and changes live in `openspec/`. Node.js is **only** needed for OpenSpec — there is no TypeScript development in this repo.

### First-time setup

Requires [Volta](https://volta.sh/) and [Bun](https://bun.sh/). Volta pins the Node.js version automatically via `package.json`.

```bash
# Install Bun (if not already installed)
volta install bun

# Install OpenSpec and its dependencies
bun install
```

### Running OpenSpec

```bash
bunx openspec
```

### Project layout

- `openspec/config.yaml` — project context and per-artifact rules passed to the AI
- `openspec/specs/` — generated spec artifacts
- `openspec/changes/` — change records (archived under `changes/archive/`)

## Build and Development Commands

### Compilation
```bash
cd src
rm -f ./dbrestore          # Clean old binary before rebuilding
go build                   # Creates 'dbrestore' executable
```

### Running Tests
```bash
cd src
go test -v ./...           # Run all tests
go test -v -skip "TestCreateTestDatabase" ./...  # Skip database-dependent tests (used in CI)
```

**Testing Requirements:**
Tests that interact with PostgreSQL require:
- Local PostgreSQL server running on localhost:5432 with user `postgres`
- Configuration file: `src/.test_config.yaml` containing the password:
  ```yaml
  password: YOUR_PASSWORD_HERE
  ```

### Dependency Management
```bash
cd src
go mod tidy                # Update go.mod and go.sum
go get -u                  # Update all dependencies to latest versions
govulncheck ./...          # Check for security vulnerabilities
```

### Running the Application
```bash
cd src
./dbrestore --help         # Show all command-line options
```

**Common usage patterns:**
- List available databases: `./dbrestore --dir /path/to/export --list`
- Restore with local files: `./dbrestore --dir /path/to/export --db-name mydb --db-user postgres --db-password secret`
- Truncate before restore: `./dbrestore --dir /path/to/export --db-name mydb --truncate-all`
- Include/exclude tables: `./dbrestore --dir /path/to/export --include-tables table1,table2 --db-name mydb`
- Skip non-empty tables: `./dbrestore --dir /path/to/export --skip-not-empty --db-name mydb`

## Architecture Overview

### Data Flow
1. **Configuration Loading** (`config/`) - Loads settings from command-line arguments, environment variables
2. **Source Layer** (`source/`) - Abstracts file access (local or S3)
3. **Parquet Reading** - Parses Parquet files and extracts table metadata (via AWS RDS export manifest files in JSON)
4. **Table Ordering** (`dag/`) - Builds dependency graph from foreign keys and performs topological sort
5. **Data Writing** (`target/`) - Connects to PostgreSQL and loads data with proper constraint/trigger management

### Key Packages

**`source/` - Data Source Abstraction**
- `source.go` - Defines the `Source` interface for file access (local or remote)
- `source_local.go` - Implements local filesystem access
- `source_s3.go` - Stub for S3 implementation (not yet complete)
- `source_reader.go` - Reads RDS export manifests and lists available tables
- `parquet_reader.go` - Implements `pgx.CopyFromSource` interface to stream Parquet data

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
- Priority order: defaults → environment variables → config file → command-line arguments
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

### Testing Strategy

- Unit tests exist for DAG sorting, CSV utilities, and database operations
- Database tests require local PostgreSQL and are skipped in CI
- DAG tests validate topological sorting with complex dependency scenarios

## Known Limitations & Future Work

From README.md:
1. **Partitioned tables** - Not yet supported
2. **PostGIS data types** - Limited support
3. **S3 loading** - Stub only, not implemented
4. **DAG cycles** - Cannot handle cyclic foreign key references
5. **Limited platforms** - Binaries not yet built for macOS arm64, Ubuntu amd64, Windows amd64

## Project Status

- Version: 0.1 (see `version.yaml`)
- **Not production-ready** - Use with test data only
- Active development with open issues on GitHub

