-- =============================================================
-- Validation Oracle for Test Seed Data
--
-- DUAL USE:
--   1. Post-seed verification: run immediately after applying
--      testdata/test_data.sql to confirm the seed succeeded.
--   2. Post-restore acceptance test: run after a future Parquet
--      export/restore round-trip to confirm all data was
--      preserved correctly (no assumptions about how data arrived).
--
-- USAGE:
--   psql -d test_comprehensive -f testdata/test_validate.sql
--
-- On success: all DO blocks complete without error, psql exits 0.
-- On failure: the first failing ASSERT raises an exception,
--             psql prints an error and exits non-zero.
--
-- PREREQUISITES:
--   testdata/test_schema.sql and testdata/test_data.sql must have
--   been applied to the target database before running this file.
-- =============================================================

\set ON_ERROR_STOP on


-- =============================================================
-- Section 4.2: Row-count checks for every seeded table
-- =============================================================

DO $$ BEGIN
    ASSERT (SELECT COUNT(*) FROM all_scalar_types) <= 1,
        'all_scalar_types must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM spatial_features) >= 1,
        'spatial_features must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM key_value_store) >= 1,
        'key_value_store must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM serial_examples) >= 1,
        'serial_examples must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM identity_examples) >= 1,
        'identity_examples must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM custom_seq_table) >= 1,
        'custom_seq_table must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM audit_log) >= 1,
        'audit_log must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM base_entity) >= 1,
        'base_entity must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM session_cache) >= 1,
        'session_cache must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM parents) >= 1,
        'parents must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM children) >= 1,
        'children must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM profiles) >= 1,
        'profiles must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM tags) >= 1,
        'tags must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM parent_tags) >= 1,
        'parent_tags must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM categories) >= 1,
        'categories must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM node_a) >= 1,
        'node_a must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM node_b) >= 1,
        'node_b must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM parent_bookings) >= 1,
        'parent_bookings must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM entity_type_a) >= 1,
        'entity_type_a must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM entity_type_b) >= 1,
        'entity_type_b must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM app.users) >= 1,
        'app.users must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM app.orders) >= 1,
        'app.orders must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM app.settings) >= 1,
        'app.settings must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM "quoted table name") >= 1,
        '"quoted table name" must have >= 1 row';
END $$;

-- Partitioned parent tables
DO $$ BEGIN
    ASSERT (SELECT COUNT(*) FROM events) >= 1,
        'events partitioned parent must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM regional_data) >= 1,
        'regional_data partitioned parent must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM sharded_records) >= 1,
        'sharded_records partitioned parent must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM sub_partitioned) >= 1,
        'sub_partitioned partitioned parent must have >= 1 row';
END $$;

-- Leaf partition checks for range-partitioned events
DO $$ BEGIN
    ASSERT (SELECT COUNT(*) FROM events_2023) >= 1,
        'events_2023 leaf partition must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM events_2024) >= 1,
        'events_2024 leaf partition must have >= 1 row';
END $$;

-- Leaf partition checks for list-partitioned regional_data
DO $$ BEGIN
    ASSERT (SELECT COUNT(*) FROM regional_data_north) >= 1,
        'regional_data_north leaf partition must have >= 1 row';
    ASSERT (SELECT COUNT(*) FROM regional_data_south) >= 1,
        'regional_data_south leaf partition must have >= 1 row';
END $$;

-- Leaf partition check for hash-partitioned sharded_records (at least one shard has data)
DO $$ BEGIN
    ASSERT (
        (SELECT COUNT(*) FROM sharded_records_0) +
        (SELECT COUNT(*) FROM sharded_records_1) +
        (SELECT COUNT(*) FROM sharded_records_2) +
        (SELECT COUNT(*) FROM sharded_records_3)
    ) >= 1,
        'at least one sharded_records leaf partition must have data';
END $$;

-- Leaf partition checks for sub-partitioned (range→hash): at least one shard per range
DO $$ BEGIN
    ASSERT (
        (SELECT COUNT(*) FROM sub_partitioned_2024_0) +
        (SELECT COUNT(*) FROM sub_partitioned_2024_1)
    ) >= 1,
        'at least one 2024 leaf of sub_partitioned must have data';
    ASSERT (
        (SELECT COUNT(*) FROM sub_partitioned_2025_0) +
        (SELECT COUNT(*) FROM sub_partitioned_2025_1)
    ) >= 1,
        'at least one 2025 leaf of sub_partitioned must have data';
END $$;


-- =============================================================
-- Section 4.3: Fully-populated row in all_scalar_types
-- Key non-DEFAULT columns must all be non-NULL in at least one row.
-- =============================================================

DO $$ BEGIN
    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_smallint    IS NOT NULL
          AND col_integer      IS NOT NULL
          AND col_bigint       IS NOT NULL
          AND col_numeric      IS NOT NULL
          AND col_real         IS NOT NULL
          AND col_double       IS NOT NULL
          AND col_text         IS NOT NULL
          AND col_boolean      IS NOT NULL
          AND col_date         IS NOT NULL
          AND col_uuid         IS NOT NULL
          AND col_inet         IS NOT NULL
          AND col_jsonb        IS NOT NULL
    ) >= 1,
        'all_scalar_types must have at least one fully-populated row';
END $$;


-- =============================================================
-- Section 4.4: FK integrity checks
-- =============================================================

DO $$ BEGIN
    ASSERT (
        SELECT COUNT(*) FROM children c
        JOIN parents p ON c.parent_id = p.id
    ) >= 1,
        'children must have rows referencing parents via FK';

    ASSERT (
        SELECT COUNT(*) FROM node_a a
        JOIN node_b b ON a.node_b_id = b.id
    ) >= 1,
        'node_a must cross-reference node_b (circular FK intact)';
END $$;


-- =============================================================
-- Section 4.5: Custom type column checks (enum, domain, composite)
-- =============================================================

DO $$ BEGIN
    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_status   IS NOT NULL
          AND col_region   IS NOT NULL
          AND col_priority IS NOT NULL
    ) >= 1,
        'at least one row must have non-NULL enum columns col_status, col_region, col_priority';

    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_pos_int > 0
          AND col_email LIKE '%@%'
    ) >= 1,
        'at least one row must satisfy domain constraints: col_pos_int > 0 and col_email LIKE %@%';

    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_address IS NOT NULL
          AND (col_address).street IS NOT NULL
    ) >= 1,
        'at least one row must have a non-NULL composite col_address with non-NULL street field';
END $$;


-- =============================================================
-- Section 4.6: Array column checks (all five named array columns)
-- =============================================================

DO $$ BEGIN
    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE array_length(col_int_array, 1) >= 1
    ) >= 1,
        'col_int_array must have at least one non-empty row';

    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE array_length(col_text_array, 1) >= 1
    ) >= 1,
        'col_text_array must have at least one non-empty row';

    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE array_length(col_uuid_array, 1) >= 1
    ) >= 1,
        'col_uuid_array must have at least one non-empty row';

    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE array_length(col_jsonb_array, 1) >= 1
    ) >= 1,
        'col_jsonb_array must have at least one non-empty row';

    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE array_length(col_numeric_array, 1) >= 1
    ) >= 1,
        'col_numeric_array must have at least one non-empty row';
END $$;


-- =============================================================
-- Section 4.7: NULL edge case checks
-- =============================================================

DO $$ BEGIN
    -- All-NULL row: key nullable columns without defaults must all be NULL
    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_smallint   IS NULL
          AND col_text       IS NULL
          AND col_boolean    IS NULL
          AND col_int_array  IS NULL
    ) >= 1,
        'an all-NULL row must exist (col_smallint, col_text, col_boolean, col_int_array all NULL)';

    -- NULL array and empty array must coexist as distinct rows
    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_int_array IS NULL
    ) >= 1,
        'at least one row must have col_int_array IS NULL (null array)';

    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_int_array = '{}'
    ) >= 1,
        'at least one row must have col_int_array = ''{'' (empty array, distinct from NULL)';
END $$;


-- =============================================================
-- Section 4.8: Special floating-point value checks
-- =============================================================

DO $$ BEGIN
    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_real = 'Infinity'::real
    ) >= 1,
        'col_real must have at least one Infinity row';

    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_real = '-Infinity'::real
    ) >= 1,
        'col_real must have at least one -Infinity row';

    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_real = 'NaN'::real
    ) >= 1,
        'col_real must have at least one NaN row';

    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_numeric = 'NaN'::numeric
    ) >= 1,
        'col_numeric must have at least one NaN row';

    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_decimal = 'NaN'::decimal
    ) >= 1,
        'col_decimal must have at least one NaN row';
END $$;


-- =============================================================
-- Section 4.9: Long text check
-- =============================================================

DO $$ BEGIN
    ASSERT (SELECT MAX(length(col_text)) FROM all_scalar_types) > 65535,
        'at least one col_text value must exceed 65535 bytes';
END $$;


-- =============================================================
-- Section 4.10: Array edge case checks (empty and 2-D array)
-- =============================================================

DO $$ BEGIN
    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_int_array = '{}'
    ) >= 1,
        'at least one row must have an empty col_int_array';

    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE array_ndims(col_int_array) = 2
    ) >= 1,
        'at least one row must have a 2-dimensional col_int_array';
END $$;


-- =============================================================
-- Section 4.11: UTF-8 text diversity checks
-- The combining-character search uses the same NFD bytes as the
-- seed (base e + U+0301 combining acute accent).
-- =============================================================

DO $$ BEGIN
    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_text LIKE '%😀%'
    ) >= 1,
        'at least one col_text row must contain emoji (😀)';

    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_text LIKE '%中文%'
    ) >= 1,
        'at least one col_text row must contain CJK text (中文)';

    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_text LIKE '%مرحبا%'
    ) >= 1,
        'at least one col_text row must contain RTL Arabic text (مرحبا)';

    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_text LIKE E'%café%'
    ) >= 1,
        'at least one col_text row must contain café in NFD form (base e + combining acute accent)';
END $$;


-- =============================================================
-- Section 4.12: Timestamp and timezone diversity checks
-- =============================================================

DO $$ BEGIN
    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_timestamptz IS NOT NULL
    ) >= 3,
        'at least 3 rows must have non-NULL col_timestamptz (one per UTC offset)';

    ASSERT (
        SELECT COUNT(*) FROM all_scalar_types
        WHERE col_timestamp IS NOT NULL
    ) >= 1,
        'at least 1 row must have a non-NULL bare col_timestamp (no timezone)';
END $$;


-- =============================================================
-- Section 4.13: PostGIS geometry checks
-- =============================================================

DO $$ BEGIN
    ASSERT (
        SELECT COUNT(*) FROM spatial_features
        WHERE geom_point      IS NOT NULL
          AND geom_linestring IS NOT NULL
          AND geog_polygon    IS NOT NULL
          AND geom_generic    IS NOT NULL
          AND geog_generic    IS NOT NULL
    ) >= 1,
        'spatial_features must have at least one row with all five PostGIS columns non-NULL';

    ASSERT (
        SELECT COUNT(*) FROM spatial_features
        WHERE NOT ST_IsValid(geom_generic)
    ) = 0,
        'all geom_generic values in spatial_features must be valid geometries';
END $$;
