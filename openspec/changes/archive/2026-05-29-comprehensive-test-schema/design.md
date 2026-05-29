## Context

The restore tool maps Parquet column types from AWS RDS exports back to PostgreSQL types. Currently there is no canonical test schema that exercises the full range of PostgreSQL features — making it impossible to see where the tool falls short. This design covers how to structure a comprehensive DDL file that defines the complete target: every feature the tool should eventually handle. Gaps identified between the schema and the tool's current capabilities become the input for future fixes, not constraints on the schema itself.

AWS RDS for PostgreSQL supports the standard PostgreSQL feature set with minor restrictions (e.g., superuser operations are restricted; some extensions require explicit enablement). The schema must be self-contained and loadable in a standard RDS-compatible environment.

## Goals / Non-Goals

**Goals:**
- Cover every AWS-RDS-supported PostgreSQL feature as completely as possible, regardless of whether the tool currently supports it
- Cover all partitioning strategies (range, list, hash, sub-partitioning)
- Cover all auto-increment patterns (SERIAL, BIGSERIAL, IDENTITY)
- Cover the full set of relationship patterns (1:1, 1:N, M:N, self-ref, deferred circular)
- Cover advanced types: PostGIS, HSTORE, JSONB, JSON, TSVECTOR, XML, arrays, ranges, composite
- Cover diverse index types and constraint types
- Be loadable by `psql` against a stock PostgreSQL + PostGIS instance
- Serve as the canonical reference schema that drives future tool improvements
- Produce a gap report identifying which features the tool does not yet handle

**Non-Goals:**
- Loading actual row data (DDL only; data fixtures are a separate concern)
- Testing RDS-specific behaviors (backup/restore mechanics, IAM auth, etc.)
- Covering deprecated or superuser-only PostgreSQL features
- Performance testing or large-scale benchmarks

## Decisions

### Target PostgreSQL versions
**Decision:** Target PostgreSQL 14–17 (the versions currently supported by AWS RDS with active community support as of 2026). PostgreSQL 12 and 13 reached community EOL in November 2024 and November 2025 respectively and are excluded.
**Rationale:** AWS RDS eventually drops versions after community EOL; building the schema against already-EOL versions adds maintenance burden with no benefit. PostgreSQL 14 is the minimum because it introduced `IF NOT EXISTS` for custom types (`ENUM`, composite, domain) — relevant to the extension-handling decision above. Docker validation should use PostgreSQL 16 as the primary target (stable, widely deployed), with PostgreSQL 17 as a secondary check.

### Single SQL file vs. multiple files
**Decision:** Single file `testdata/comprehensive_test_schema.sql` at the repository root.
**Rationale:** Easier to apply atomically via `psql -f`; simpler for CI; no ordering ambiguity between files. The schema is not application code — splitting by concern adds navigation overhead with no real benefit.

### Fresh database vs. idempotent re-runs
**Decision:** The script targets a fresh empty database. Plain `CREATE` statements throughout — no idempotency guards except on extensions.
**Rationale:** True idempotency across the full feature set is not feasible without significant complexity: `ALTER TABLE ADD CONSTRAINT` has no `IF NOT EXISTS` equivalent, custom types (`ENUM`, composite, domain, range) require PostgreSQL 14+ for `IF NOT EXISTS`, and partition child re-attachment has no clean syntax. Workarounds (PL/pgSQL `DO` blocks querying `pg_constraint`/`pg_type`) would make the file hard to read and maintain. The schema is a reference fixture, not a migration — the correct iteration workflow is `dropdb` + `createdb` + re-apply, which takes seconds. The spec and CI setup script should document this workflow explicitly.

### Extension handling
**Decision:** Declare all required extensions at the top with `CREATE EXTENSION IF NOT EXISTS`.
Extensions needed: `postgis`, `hstore`, `uuid-ossp`, `pg_trgm`, `btree_gist`, `btree_gin`.
**Rationale:** `IF NOT EXISTS` is kept for extensions only, as a courtesy for environments where extensions are pre-installed by the DBA. All other objects use plain `CREATE`.

### Transaction wrapping
**Decision:** Wrap the entire script in a single `BEGIN` / `COMMIT`.
**Rationale:** If any DDL fails the whole schema rolls back cleanly, avoiding partial state.

### Circular / deferred foreign keys
**Decision:** Use `DEFERRABLE INITIALLY DEFERRED` constraints for the one circular reference example.
**Rationale:** PostgreSQL requires deferrable FKs to express circular references without special load ordering. This is the exact pattern the restore tool must handle.

### Partitioned tables
**Decision:** Create three partition families: range (by date), list (by region code), hash (by ID). Add one sub-partitioned table (range → hash).
**Rationale:** Covers the three native PostgreSQL partition strategies and the sub-partitioning case that the tool currently does not support, making future test failures explicit.

### PostGIS types
**Decision:** Include `geometry(Point, 4326)`, `geography(Polygon, 4326)`, and a generic `geometry` column.
**Rationale:** The CLAUDE.md notes PostGIS support is limited; this gives a concrete target to test against.

### Schema namespacing
**Decision:** Place most tables in the `public` schema (the common default) and a meaningful subset in a second schema named `app`. Include at least one cross-schema foreign key (`app` table referencing a `public` table) and a cross-schema view.
**Rationale:** AWS RDS exports include the schema name in table metadata; the restore tool must correctly resolve `schema.table` qualified names in the DAG ordering, type mapping, and COPY target. Covering both `public` and a non-public schema is the minimum needed to expose any schema-qualification bugs. `app` is chosen as a name that mirrors common real-world practice (e.g., application tables separated from shared/reference data in `public`).

### Naming convention
**Decision:** All table names use `snake_case`; types use `snake_case`; partitions are named `<parent>_<discriminator>`.
**Rationale:** Consistent with the existing test database conventions in the codebase.

## Risks / Trade-offs

- **Extension availability** → Mitigation: Guard with `IF NOT EXISTS`; document required extensions in comments; provide a setup note.
- **Circular FK complexity** → Mitigation: Use `DEFERRABLE INITIALLY DEFERRED`; document clearly in the file.
- **Partitioned table tool limitation** → Mitigation: Document in comments that these tables exercise known-unsupported behavior, so test failures are expected rather than surprising.
- **Schema drift** → Mitigation: The file is version-controlled; changes require a deliberate PR.
- **PostGIS version differences** → Mitigation: Use widely supported geometry types (Point, Polygon, LineString) and avoid PostGIS 3-only syntax.
