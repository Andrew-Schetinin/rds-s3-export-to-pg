-- =============================================================
-- Test Seed Data for Comprehensive PostgreSQL Test Schema
--
-- PREREQUISITES:
--   testdata/test_schema.sql must be applied to the target database first.
--
-- USAGE:
--   psql -d test_comprehensive -f testdata/test_data.sql
--
-- FK INSERTION ORDER:
--   1. Independent tables (no foreign keys):
--        all_scalar_types, spatial_features, key_value_store,
--        serial_examples, identity_examples, custom_seq_table,
--        audit_log, base_entity, session_cache
--   2. FK-dependent tables (topological order):
--        parents → children → profiles
--        tags → parent_tags
--        categories (self-referential, root first then children)
--   3. Circular FK pair inside a deferred transaction:
--        node_a + node_b (constraints are DEFERRABLE INITIALLY DEFERRED)
--   4. Partitioned tables (inserted via parent; PostgreSQL routes to partitions):
--        events (range), regional_data (list),
--        sharded_records (hash), sub_partitioned (range→hash)
--   5. parent_bookings (exclusion constraint, FK to parents)
--   6. app schema in order: app.users → app.orders → app.settings
--   7. Inheritance tables: entity_type_a, entity_type_b
--   8. Quoted-identifier table: "quoted table name"
--   9. Edge-case rows appended to all_scalar_types:
--        NULL row, +Infinity/-Infinity/NaN floats, long text (>65 KiB),
--        empty array, 2-D array, UTF-8 diversity (emoji, CJK, RTL,
--        combining chars), TIMESTAMPTZ rows with multiple UTC offsets
-- =============================================================


-- =============================================================
-- Section 1: Independent tables (no foreign keys)
-- =============================================================

-- all_scalar_types: one fully-populated row covering every column type
INSERT INTO all_scalar_types (
    col_smallint, col_integer, col_bigint, col_numeric, col_decimal,
    col_real, col_double, col_money,
    col_char, col_varchar, col_text,
    col_bytea,
    col_boolean,
    col_date, col_time, col_timetz, col_timestamp, col_timestamptz, col_interval,
    col_inet, col_cidr, col_macaddr, col_macaddr8,
    col_uuid,
    col_xml, col_json, col_jsonb, col_tsvector, col_tsquery,
    col_bit, col_varbit,
    col_point, col_line, col_lseg, col_box, col_path, col_polygon, col_circle,
    col_int4range, col_int8range, col_numrange, col_tsrange, col_tstzrange, col_daterange,
    col_int4multirange, col_int8multirange, col_nummultirange,
    col_tsmultirange, col_tstzmultirange, col_datemultirange,
    col_int_array, col_text_array, col_uuid_array, col_jsonb_array, col_numeric_array,
    col_status, col_region, col_priority, col_address, col_pos_int, col_email, col_float_range,
    col_amount, col_label
) VALUES (
    1,                                                              -- col_smallint
    100,                                                            -- col_integer
    1000000,                                                        -- col_bigint
    12345.6789,                                                     -- col_numeric(18,4)
    99.99,                                                          -- col_decimal(10,2)
    3.14,                                                           -- col_real
    2.718281828,                                                    -- col_double
    100.00::numeric::money,                                         -- col_money
    'hellochar',                                                    -- col_char(10)
    'hello varchar',                                                -- col_varchar(255)
    'hello text',                                                   -- col_text
    decode('deadbeef', 'hex'),                                      -- col_bytea
    TRUE,                                                           -- col_boolean
    '2024-01-15',                                                   -- col_date
    '12:30:00',                                                     -- col_time
    '12:30:00+05:30',                                               -- col_timetz
    '2024-01-15 12:30:00',                                          -- col_timestamp
    '2024-01-15 12:30:00+00:00',                                    -- col_timestamptz
    '1 year 2 months 3 days',                                       -- col_interval
    '192.168.1.100',                                                -- col_inet
    '192.168.1.0/24',                                               -- col_cidr
    '08:00:2b:01:02:03',                                            -- col_macaddr
    '08:00:2b:01:02:03:04:05',                                      -- col_macaddr8
    gen_random_uuid(),                                              -- col_uuid
    '<root><child>text</child></root>',                             -- col_xml
    '{"key": "value"}',                                             -- col_json
    '{"key": "value", "num": 42}',                                  -- col_jsonb
    to_tsvector('english', 'hello world'),                          -- col_tsvector
    to_tsquery('english', 'hello & world'),                         -- col_tsquery
    B'10101010',                                                    -- col_bit(8)
    B'1010',                                                        -- col_varbit(16)
    '(1.5,2.5)',                                                    -- col_point
    '{1,-1,0}',                                                     -- col_line (x - y = 0)
    '[(0,0),(1,1)]',                                                -- col_lseg
    '(2,2),(0,0)',                                                  -- col_box
    '((0,0),(1,1),(2,0))',                                          -- col_path
    '((0,0),(1,0),(1,1),(0,1))',                                    -- col_polygon
    '<(1,1),5>',                                                    -- col_circle
    '[1,10)',                                                        -- col_int4range
    '[1,1000)',                                                      -- col_int8range
    '[1.5,10.5)',                                                    -- col_numrange
    '[2024-01-01 00:00:00,2024-12-31 00:00:00)',                    -- col_tsrange
    '[2024-01-01 00:00:00+00,2024-12-31 00:00:00+00)',              -- col_tstzrange
    '[2024-01-01,2024-12-31)',                                       -- col_daterange
    '{[1,5),[10,20)}',                                              -- col_int4multirange
    '{[1,100),[200,300)}',                                          -- col_int8multirange
    '{[1.0,5.0),[10.0,20.0)}',                                      -- col_nummultirange
    '{[2024-01-01 00:00:00,2024-06-01 00:00:00)}',                  -- col_tsmultirange
    '{[2024-01-01 00:00:00+00,2024-06-01 00:00:00+00)}',            -- col_tstzmultirange
    '{[2024-01-01,2024-06-01)}',                                    -- col_datemultirange
    '{1,2,3}',                                                      -- col_int_array
    '{hello,world}',                                                -- col_text_array
    ARRAY[gen_random_uuid()],                                       -- col_uuid_array
    ARRAY['{"key":1}'::jsonb],                                      -- col_jsonb_array
    '{1.5,2.5,3.5}',                                                -- col_numeric_array
    'active',                                                       -- col_status (status_enum)
    'north',                                                        -- col_region (region_enum)
    'high',                                                         -- col_priority (priority_level)
    ROW('123 Main St', 'Springfield', 'IL', 'US', '62701')::address_type,  -- col_address
    42,                                                             -- col_pos_int (positive_integer domain)
    'test@example.com',                                             -- col_email (email_address domain)
    '[1.5,10.5)',                                                   -- col_float_range
    99.99,                                                          -- col_amount
    'main row'                                                      -- col_label
);

-- spatial_features: one row with non-NULL WKT values for all five PostGIS columns
INSERT INTO spatial_features (name, geom_point, geom_linestring, geog_polygon, geom_generic, geog_generic)
VALUES (
    'test feature',
    ST_GeomFromText('POINT(1 2)', 4326),
    ST_GeomFromText('LINESTRING(0 0, 1 1)', 4326),
    ST_GeogFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'),
    ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))', 4326),
    ST_GeogFromText('POINT(10 20)')
);

-- key_value_store: hstore attributes
INSERT INTO key_value_store (entity_type, entity_id, attributes)
VALUES ('user', 1, '"name"=>"Alice","role"=>"admin"');

-- serial_examples: covers SMALLSERIAL, SERIAL, BIGSERIAL
INSERT INTO serial_examples (label) VALUES ('serial example 1');

-- identity_examples: covers GENERATED ALWAYS and GENERATED BY DEFAULT AS IDENTITY
INSERT INTO identity_examples (label) VALUES ('identity example 1');

-- custom_seq_table: uses nextval('custom_id_seq') as DEFAULT
INSERT INTO custom_seq_table (label) VALUES ('custom seq 1');

-- audit_log: standalone log table with no FKs
INSERT INTO audit_log (table_name, record_id, action)
VALUES ('test_table', 1, 'INSERT');

-- base_entity: direct insert (entity_type_a/b rows also appear here via inheritance)
INSERT INTO base_entity (is_active) VALUES (TRUE);

-- session_cache: UNLOGGED table with UUID PK default
INSERT INTO session_cache (user_data, expires_at)
VALUES ('{"session": "active", "user": "test"}'::jsonb, NOW() + INTERVAL '1 hour');


-- =============================================================
-- Section 2: FK-dependent tables (topological order)
-- =============================================================

-- parents (root of several FK chains)
INSERT INTO parents (name) VALUES ('Parent One'), ('Parent Two');

-- children (FK → parents); birth_year is GENERATED ALWAYS — omit from INSERT
INSERT INTO children (parent_id, name, birth_date)
VALUES (1, 'Child One', '2010-05-15');

-- profiles: one-to-one FK → parents (UNIQUE constraint on parent_id)
INSERT INTO profiles (parent_id, bio, avatar_url)
VALUES (1, 'Biography for Parent One', 'https://example.com/avatar1.jpg');

-- tags
INSERT INTO tags (name, slug)
VALUES ('Tag One', 'tag-one'), ('Tag Two', 'tag-two');

-- parent_tags: many-to-many join table (FK → parents, FK → tags)
INSERT INTO parent_tags (parent_id, tag_id)
VALUES (1, 1), (1, 2), (2, 1);

-- categories: self-referential FK; insert root first, then children
INSERT INTO categories (name, depth) VALUES ('Root Category', 0);
INSERT INTO categories (parent_id, name, depth) VALUES (1, 'Sub Category', 1);


-- =============================================================
-- Section 3: Circular FK pair (node_a ↔ node_b)
-- Both constraints are DEFERRABLE INITIALLY DEFERRED, so they are
-- only checked at COMMIT time. The UPDATE closes the cycle.
-- =============================================================

BEGIN;
SET CONSTRAINTS ALL DEFERRED;
INSERT INTO node_a (label, node_b_id) VALUES ('alpha', NULL);
INSERT INTO node_b (label, node_a_id) VALUES ('beta', currval('node_a_id_seq'));
UPDATE node_a SET node_b_id = currval('node_b_id_seq')
WHERE id = currval('node_a_id_seq');
COMMIT;


-- =============================================================
-- Section 4: Partitioned tables (inserted via parent table)
-- =============================================================

-- events: range-partitioned by event_date; cover at least two partition ranges
INSERT INTO events (event_date, title, region, payload) VALUES
    ('2023-06-15', 'Conference 2023', 'north', '{"type": "conference"}'),
    ('2024-03-20', 'Meetup 2024',     'south', '{"type": "meetup"}'),
    ('2025-09-10', 'Workshop 2025',   'east',  '{"type": "workshop"}');

-- regional_data: list-partitioned by region text; cover at least two regions
INSERT INTO regional_data (region, payload) VALUES
    ('north', '{"data": "northern region data"}'),
    ('south', '{"data": "southern region data"}');

-- sharded_records: hash-partitioned by id; insert several rows to spread across shards
INSERT INTO sharded_records (payload) VALUES
    ('shard record A'),
    ('shard record B'),
    ('shard record C'),
    ('shard record D');

-- sub_partitioned: range(created_at) → hash(id); cover 2024 and 2025 sub-partitions
INSERT INTO sub_partitioned (created_at, payload) VALUES
    ('2024-03-15 10:00:00+00', 'Q1 2024 record'),
    ('2024-09-20 14:00:00+00', 'Q3 2024 record'),
    ('2025-04-10 08:00:00+00', 'Q2 2025 record'),
    ('2025-11-05 16:00:00+00', 'Q4 2025 record');


-- =============================================================
-- Section 5: Exclusion constraint table
-- Non-overlapping date ranges for the same parent_id.
-- =============================================================

INSERT INTO parent_bookings (parent_id, during) VALUES
    (1, '[2024-01-01,2024-03-31)'),
    (1, '[2024-04-01,2024-06-30)');


-- =============================================================
-- Section 6: app schema (cross-schema FK ordering)
-- =============================================================

-- app.users (no FKs; uuid PK has DEFAULT gen_random_uuid())
INSERT INTO app.users (name, email, status)
VALUES ('Alice Smith', 'alice@example.com', 'active');

-- app.orders (FK → app.users, FK → public.parents)
INSERT INTO app.orders (user_id, parent_id, amount, region)
SELECT u.id, 1, 150.00, 'north'
FROM app.users u WHERE u.email = 'alice@example.com';

-- app.settings (no FKs)
INSERT INTO app.settings (key, value)
VALUES ('theme', '"dark"'), ('notifications', 'true');


-- =============================================================
-- Section 7: Inheritance tables
-- Each insert populates both the child table and base_entity.
-- =============================================================

INSERT INTO entity_type_a (label, priority) VALUES ('Entity A One', 'high');
INSERT INTO entity_type_b (score, tags) VALUES (95.5000, '{tag1,tag2}');


-- =============================================================
-- Section 8: Quoted-identifier table
-- =============================================================

INSERT INTO "quoted table name" ("col with spaces", "another col")
VALUES ('some text value', 42);


-- =============================================================
-- Section 9: Edge-case rows in all_scalar_types
-- =============================================================

-- 3.1 All-NULL row: only id (BIGSERIAL) is populated; all nullable columns are NULL
INSERT INTO all_scalar_types DEFAULT VALUES;

-- 3.2 Positive infinity for real and double precision
INSERT INTO all_scalar_types (col_real, col_double, col_label)
VALUES ('Infinity'::real, 'Infinity'::double precision, 'positive infinity');

-- 3.3 Negative infinity for real and double precision
INSERT INTO all_scalar_types (col_real, col_double, col_label)
VALUES ('-Infinity'::real, '-Infinity'::double precision, 'negative infinity');

-- 3.4 NaN for real, double precision, and numeric
INSERT INTO all_scalar_types (col_real, col_double, col_numeric, col_label)
VALUES ('NaN'::real, 'NaN'::double precision, 'NaN'::numeric, 'nan');

-- 3.5 Long text: col_text exceeds 65,535 bytes
INSERT INTO all_scalar_types (col_text, col_label)
VALUES (repeat('x', 70000), 'long text > 65535 bytes');

-- 3.6 Empty array for col_int_array; col_text_array left NULL (distinct from empty)
INSERT INTO all_scalar_types (col_int_array, col_label)
VALUES ('{}', 'empty int array with null text array');

-- 3.7 Two-dimensional integer array
INSERT INTO all_scalar_types (col_int_array, col_label)
VALUES ('{{1,2},{3,4}}'::integer[], '2d integer array');

-- 3.8 Emoji text (UTF-8 4-byte sequences)
INSERT INTO all_scalar_types (col_text, col_label)
VALUES ('Hello 😀🌍', 'emoji text');

-- 3.8b CJK text
INSERT INTO all_scalar_types (col_text, col_label)
VALUES ('日本語 中文 한국어', 'cjk text');

-- 3.9 RTL text (Arabic)
INSERT INTO all_scalar_types (col_text, col_label)
VALUES ('مرحبا بالعالم', 'rtl arabic');

-- 3.9b Combining characters: café stored in NFD (base e + U+0301 combining acute accent)
INSERT INTO all_scalar_types (col_text, col_label)
VALUES (E'café', 'combining accent nfd');

-- 3.10 TIMESTAMPTZ rows covering three UTC offsets; first row also sets col_timestamp
INSERT INTO all_scalar_types (col_timestamptz, col_timestamp, col_label)
VALUES ('2024-06-15 12:00:00+00:00', '2024-06-15 12:00:00', 'timestamptz +00:00 with bare timestamp');

INSERT INTO all_scalar_types (col_timestamptz, col_label)
VALUES ('2024-06-15 17:30:00+05:30', 'timestamptz +05:30');

INSERT INTO all_scalar_types (col_timestamptz, col_label)
VALUES ('2024-06-15 04:00:00-08:00', 'timestamptz -08:00');
