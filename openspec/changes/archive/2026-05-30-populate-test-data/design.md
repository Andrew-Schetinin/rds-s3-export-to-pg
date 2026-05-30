## Context

`testdata/test_schema.sql` (added in #14/#15) defines a full-coverage PostgreSQL schema — enums, domains, composite types, every scalar type, FK patterns including circular, and partitioned tables. No seed data exists alongside it, so the schema cannot be exercised in any meaningful data-loading test.

This change delivers three standalone SQL files — an updated schema, a seed file, and a validation oracle — all applicable with `psql -f` independently of any Go test harness.

## Goals / Non-Goals

**Goals:**
- Add a quoted-identifier table to `test_schema.sql`
- Provide `testdata/test_data.sql` with INSERT rows for every table, including partitioned tables (inserted via parent tables so PostgreSQL routes rows to partitions)
- Cover all FK dependency orderings (parents before children, deferred for circular)
- Represent edge-case values: explicit NULL, `Infinity`/`-Infinity`/`NaN` for floats, text >64 KiB, empty arrays, 2D arrays, UTF-8 diversity (emoji, CJK, RTL, combining chars), and multi-timezone `TIMESTAMPTZ` values
- Provide `testdata/test_validate.sql` as a reusable validation oracle for both seed verification and future restore round-trip testing

**Non-Goals:**
- End-to-end Parquet round-trip testing (depends on a future Parquet generation change)

## Decisions

### Decision: Quoted-identifier table added to the schema, not a separate file

The quoted-identifier table (`"quoted table name"`) belongs in `test_schema.sql` alongside the rest of the schema so it is created and dropped as a unit. It is a small addition (one `CREATE TABLE`) and fits the existing schema's "comprehensive coverage" intent.

### Decision: Edge-case values live in additional rows, not a separate seed file

Each edge-case category (NULL row, Infinity row, long-text row, etc.) is an extra `INSERT` in `test_data.sql` targeting `all_scalar_types`. This keeps all seed data in one file while still making edge-case rows individually identifiable (e.g. `WHERE col_real = 'Infinity'`).

*Alternative considered*: A separate `edge_cases_test_data.sql`. Rejected because it adds a third file to load in order and splits related data.

### Decision: Separate validation oracle file using PL/pgSQL ASSERT

`test_validate.sql` contains anonymous `DO $$ BEGIN ASSERT ...; END $$;` blocks that check for the presence and correctness of specific known values. Running this file after seeding confirms the data arrived intact; running it after a future Parquet restore confirms the round-trip was lossless. This dual-use makes the file the natural acceptance test for the restore pipeline without requiring any Go code.

*Alternative considered*: Embedding assertions inline in the seed file (assertions immediately after each INSERT group). Rejected because it prevents running the seed without the validation, and complicates future use as a post-restore oracle.

*Alternative considered*: Generating the validate file from the seed file using a script. Rejected as over-engineering for the current scope; the validate file can be maintained by hand alongside the seed.

### Decision: Plain SQL seed file, not Go-generated fixtures

A `.sql` file in `testdata/` mirrors how the schema file is stored and can be applied with `psql -f` independently of the test suite. Go-generated fixtures would couple data shape to Go struct definitions, making schema changes harder to maintain.

*Alternative considered*: Go test helpers that insert rows via `pgx`. Rejected because the SQL file is reviewable, reproducible outside Go, and matches existing conventions.

### Decision: Manually respect FK insertion order in the seed file

The seed file inserts rows in the same topological order the restore tool computes at runtime. This makes the file self-contained and also serves as a human-readable illustration of the expected load order.

For the circular FK between `node_a` and `node_b`, inserts are wrapped in a `BEGIN`/`COMMIT` with `SET CONSTRAINTS ALL DEFERRED`.

## Risks / Trade-offs

- **Schema drift** → Both the seed and validate files must be updated whenever `test_schema.sql` adds or removes columns. Mitigated by applying all three files together during local verification before committing.
- **Circular FK deferred transaction** → If `SET CONSTRAINTS ALL DEFERRED` is not effective for a particular constraint, the insert will fail. Mitigated by testing the seed file locally before committing.

## Open Questions

- None at this time.
