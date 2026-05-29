## 1. Schema Update

- [ ] 1.1 Add a quoted-identifier table to `testdata/test_schema.sql`: `"quoted table name"` with columns `"id col"` (BIGSERIAL PK), `"col with spaces"` (TEXT), and `"another col"` (INTEGER)

## 2. SQL Seed File — Base Coverage

- [ ] 2.1 Create `testdata/test_data.sql` with a header comment describing load prerequisites and FK ordering
- [ ] 2.2 Insert rows for independent tables with no FKs: `all_scalar_types`, `key_value_store`, `serial_examples`, `identity_examples`, `custom_seq_table`, `audit_log`, `base_entity`
- [ ] 2.2a Insert one row into `spatial_features` with WKT values for all five PostGIS columns: `ST_GeomFromText('POINT(1 2)',4326)`, `ST_GeomFromText('LINESTRING(0 0,1 1)',4326)`, `ST_GeogFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))')`, and generic geometry/geography variants
- [ ] 2.3 Insert rows for tables in FK dependency order: `parents` → `children` → `profiles`; `tags` → `parent_tags`; `categories` (self-referential, root first)
- [ ] 2.4 Insert the circular FK rows for `node_a` and `node_b` inside a single `BEGIN … SET CONSTRAINTS ALL DEFERRED … COMMIT` block
- [ ] 2.5 Insert rows into partitioned parent tables via the parent (PostgreSQL routes to partitions): `events` (range — cover at least two partition ranges), `regional_data` (list — cover at least two regions), `sharded_records` (hash), `sub_partitioned` (range→hash, cover at least two sub-partitions)
- [ ] 2.6 Insert rows for `parent_bookings` (exclusion constraint table) with non-overlapping ranges
- [ ] 2.7 Insert rows for `app.users`, then `app.orders` (FK to `app.users` and to `parents`), then `app.settings`
- [ ] 2.8 Insert rows for inheritance tables: `entity_type_a`, `entity_type_b` (these also populate `base_entity` via inheritance)
- [ ] 2.9 Insert one row into `"quoted table name"` with non-NULL values for all columns

## 3. SQL Seed File — Edge Cases in all_scalar_types

- [ ] 3.1 Insert an all-NULL row (only `id` set) to cover the NULL path for every nullable column
- [ ] 3.2 Insert a row with `col_real = 'Infinity'::real`, `col_double = 'Infinity'::double precision`
- [ ] 3.3 Insert a row with `col_real = '-Infinity'::real`, `col_double = '-Infinity'::double precision`
- [ ] 3.4 Insert a row with `col_real = 'NaN'::real`, `col_double = 'NaN'::double precision`, `col_numeric = 'NaN'::numeric`
- [ ] 3.5 Insert a row where `col_text` is a string longer than 65,535 bytes (generate via `repeat('x', 70000)` or equivalent)
- [ ] 3.6 Insert a row where `col_int_array = '{}'` (empty array) and `col_text_array` IS NULL (null array — distinct from empty)
- [ ] 3.7 Insert a row where `col_int_array = '{{1,2},{3,4}}'::integer[]` (two-dimensional array)
- [ ] 3.8 Insert a row where `col_text` contains emoji (e.g. `'Hello 😀🌍'`), and another row with CJK text (e.g. `'日本語 中文 한국어'`)
- [ ] 3.9 Insert a row where `col_text` contains RTL text (e.g. Arabic `'مرحبا بالعالم'`) and a row with combining characters (e.g. `'café'` using base `e` + combining accent)
- [ ] 3.10 Insert rows with `col_timestamptz` expressed in at least three different UTC offsets (`+00:00`, `+05:30`, `-08:00`) and at least one row with a bare `col_timestamp` value (no timezone)

## 4. SQL Validation Oracle

- [ ] 4.1 Create `testdata/test_validate.sql` with a header comment explaining its dual use (post-seed verification and future post-restore acceptance test)
- [ ] 4.2 Add `DO $$ BEGIN ASSERT ... END $$;` blocks asserting `COUNT(*) >= 1` for every seeded table, including all four partitioned parents and a leaf partition for each, and `"quoted table name"`
- [ ] 4.3 Add ASSERT: fully-populated row in `all_scalar_types` (key non-DEFAULT columns all non-NULL in at least one row)
- [ ] 4.4 Add ASSERTs for FK integrity: `children JOIN parents` returns ≥ 1 row; `node_a JOIN node_b` on circular reference returns ≥ 1 row
- [ ] 4.5 Add ASSERTs for custom types: `col_status`, `col_region`, `col_priority` non-NULL; `col_pos_int > 0` and `col_email LIKE '%@%'`; `col_address IS NOT NULL AND (col_address).street IS NOT NULL`
- [ ] 4.6 Add ASSERTs for all five named array columns having at least one non-empty row (`array_length >= 1`)
- [ ] 4.7 Add ASSERTs for NULL edge cases: all-NULL row present; `col_int_array IS NULL` row and `col_int_array = '{}'` row coexist as distinct rows
- [ ] 4.8 Add ASSERTs for special float values: `Infinity`, `-Infinity`, `NaN` for `col_real`; `NaN` for `col_numeric`
- [ ] 4.9 Add ASSERT: `MAX(length(col_text)) > 65535`
- [ ] 4.10 Add ASSERTs for array edge cases: empty array row and 2D array row
- [ ] 4.11 Add ASSERTs for all four UTF-8 categories: emoji (`😀`), CJK (`中文`), RTL (`مرحبا`), combining chars (`café` as base+accent)
- [ ] 4.12 Add ASSERTs for timestamps: `col_timestamptz IS NOT NULL` count ≥ 3; `col_timestamp IS NOT NULL` count ≥ 1
- [ ] 4.13 Add ASSERTs for PostGIS: all five columns non-NULL in at least one row; `COUNT(*) WHERE NOT ST_IsValid(geom_generic) = 0`

## 5. Validation

- [ ] 5.1 Apply all three SQL files manually against a local test database in order: schema → seed → validate, and confirm no errors
- [ ] 5.2 Spot-check: drop the database, re-create, apply schema only (no seed), run validate — confirm it exits non-zero, proving the oracle detects missing data
