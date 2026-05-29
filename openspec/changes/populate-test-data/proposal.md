## Why

The comprehensive test schema (`testdata/test_schema.sql`) defines every PostgreSQL type, relationship, and partitioning pattern that the restore tool must handle, but no seed data exists — leaving integration tests unable to exercise the actual restore pipeline end-to-end. Populating the schema with representative rows unblocks true integration testing of the data-loading path and pre-positions the project for when partitioned-table support and the full restore round-trip are implemented.

## What Changes

- Add `testdata/test_data.sql` — a seed script with INSERT statements covering all tables in the comprehensive schema (including partitioned tables, inserting into parent tables so PostgreSQL routes rows to partitions). Includes all scalar types, custom types, enum/domain/composite columns, FK relationships (including self-referential and circular), and array columns. The seed deliberately includes edge-case values: explicit NULLs in nullable columns; `Infinity`, `-Infinity`, and `NaN` for float columns; text values exceeding 65,535 bytes; empty (`'{}'`) and multi-dimensional arrays; UTF-8 text spanning emoji, CJK, RTL, and combining-character sequences; and `TIMESTAMPTZ` values expressed in multiple UTC offsets alongside bare `TIMESTAMP` values.
- Add `testdata/test_validate.sql` — a validation oracle using PL/pgSQL `DO $$ BEGIN ASSERT ... END $$;` blocks that confirm specific known values are present and correct after the seed is applied. This file is designed to be run in two contexts: immediately after seeding (to verify the seed itself), and after a future Parquet export/restore round-trip (to verify the restore pipeline preserved data correctly).
- Add a quoted-identifier table to `testdata/test_schema.sql` — a table whose name and column names contain spaces (e.g. `"quoted table name"`) — to verify the restore tool handles PostgreSQL quoted identifiers correctly, provided AWS RDS exports them.

## Capabilities

### New Capabilities
- `test-data`: SQL seed file that populates every table in the comprehensive test schema with at least one representative row per column type or relationship pattern, including all edge-case value categories.
- `test-validate`: SQL validation oracle that asserts specific seeded values are present and correct; reusable as the acceptance check for the future restore round-trip integration test.

### Modified Capabilities
- `test-schema`: Add a quoted-identifier table to the existing schema file.

## Impact

- `testdata/test_data.sql` — new seed file
- `testdata/test_validate.sql` — new validation oracle
- `testdata/test_schema.sql` — add quoted-identifier table
- No Go source changes; no CI configuration changes

## Non-goals

- Generating Parquet files from the test data (a separate future change)
- Automated export/import round-trip verification (depends on Parquet generation)
- Exhaustive PostGIS type coverage (only the five columns in `spatial_features` are seeded)
