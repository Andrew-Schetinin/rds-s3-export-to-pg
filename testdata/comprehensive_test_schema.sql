-- =============================================================
-- Comprehensive PostgreSQL Test Schema
--
-- Covers every AWS-RDS-supported PostgreSQL feature relevant to
-- the rds-s3-export-to-pg restore tool. Use this schema as the
-- canonical target: gaps between this schema and what the tool
-- handles are bugs to fix in the tool, not omissions here.
--
-- Tested on: PostgreSQL 16 (postgis/postgis:16-3.4)
--            PostgreSQL 17 (postgis/postgis:17-3.5)
--
-- USAGE (fresh database required — not idempotent):
--   createdb test_comprehensive
--   psql -d test_comprehensive -f testdata/comprehensive_test_schema.sql
--
-- To reset: dropdb test_comprehensive && createdb test_comprehensive
--
-- REQUIRED EXTENSIONS (must be available in the PostgreSQL installation):
--   postgis, hstore, uuid-ossp, pg_trgm, btree_gist, btree_gin
--
-- On AWS RDS these are enabled via:
--   CREATE EXTENSION ... (as rds_superuser, no special parameter group needed)
-- =============================================================

BEGIN;


-- =============================================================
-- EXTENSIONS
-- IF NOT EXISTS is used only here: extensions may be pre-installed
-- by the DBA. All other objects use plain CREATE (fresh DB assumed).
-- =============================================================
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS btree_gin;


-- =============================================================
-- ENUM TYPES
-- =============================================================
CREATE TYPE status_enum AS ENUM ('active', 'inactive', 'pending', 'deleted');
CREATE TYPE region_enum AS ENUM ('north', 'south', 'east', 'west', 'central');
CREATE TYPE priority_level AS ENUM ('low', 'medium', 'high', 'critical');


-- =============================================================
-- COMPOSITE TYPE
-- =============================================================
CREATE TYPE address_type AS (
    street   TEXT,
    city     TEXT,
    state    TEXT,
    country  TEXT,
    zip_code VARCHAR(20)
);


-- =============================================================
-- DOMAIN TYPES
-- =============================================================
CREATE DOMAIN positive_integer AS INTEGER
    CHECK (VALUE > 0);

CREATE DOMAIN email_address AS TEXT
    CHECK (VALUE ~ '^[^@\s]+@[^@\s]+\.[^@\s]+$');


-- =============================================================
-- CUSTOM RANGE TYPE
-- =============================================================
CREATE TYPE float8_range AS RANGE (subtype = FLOAT8);


-- =============================================================
-- STANDALONE SEQUENCE
-- Referenced via DEFAULT nextval(...) in custom_seq_table.
-- =============================================================
CREATE SEQUENCE custom_id_seq
    START    1000
    INCREMENT   5
    MINVALUE 1000;


-- =============================================================
-- ALL SCALAR TYPES
-- One table exercising every scalar type the tool must handle.
-- =============================================================
CREATE TABLE all_scalar_types (
    id                BIGSERIAL PRIMARY KEY,

    -- Numeric
    col_smallint      SMALLINT,
    col_integer       INTEGER,
    col_bigint        BIGINT,
    col_numeric       NUMERIC(18, 4),
    col_decimal       DECIMAL(10, 2),
    col_real          REAL,
    col_double        DOUBLE PRECISION,
    col_money         MONEY,

    -- Text
    col_char          CHAR(10),
    col_varchar       VARCHAR(255),
    col_text          TEXT,

    -- Binary
    col_bytea         BYTEA,

    -- Boolean
    col_boolean       BOOLEAN,

    -- Temporal
    col_date          DATE,
    col_time          TIME,
    col_timetz        TIMETZ,
    col_timestamp     TIMESTAMP,
    col_timestamptz   TIMESTAMPTZ,
    col_interval      INTERVAL,

    -- Network / address
    col_inet          INET,
    col_cidr          CIDR,
    col_macaddr       MACADDR,
    col_macaddr8      MACADDR8,

    -- UUID
    col_uuid          UUID,

    -- Document / search
    col_xml           XML,
    col_json          JSON,
    col_jsonb         JSONB,
    col_tsvector      TSVECTOR,
    col_tsquery       TSQUERY,

    -- Bit strings
    col_bit           BIT(8),
    col_varbit        VARBIT(16),

    -- Native geometric (non-PostGIS)
    col_point         POINT,
    col_line          LINE,
    col_lseg          LSEG,
    col_box           BOX,
    col_path          PATH,
    col_polygon       POLYGON,
    col_circle        CIRCLE,

    -- Built-in range types
    col_int4range     INT4RANGE,
    col_int8range     INT8RANGE,
    col_numrange      NUMRANGE,
    col_tsrange       TSRANGE,
    col_tstzrange     TSTZRANGE,
    col_daterange     DATERANGE,

    -- Array types
    col_int_array     INTEGER[],
    col_text_array    TEXT[],
    col_uuid_array    UUID[],
    col_jsonb_array   JSONB[],
    col_numeric_array NUMERIC[],

    -- Custom / user-defined types
    col_status        status_enum,
    col_region        region_enum,
    col_priority      priority_level,
    col_address       address_type,
    col_pos_int       positive_integer,
    col_email         email_address,
    col_float_range   float8_range,

    -- Defaults and check constraints
    col_amount        NUMERIC(12, 2)  DEFAULT 0.00 CHECK (col_amount >= 0),
    col_label         TEXT            CHECK (col_label <> ''),
    col_uid           UUID            DEFAULT gen_random_uuid(),
    col_created_at    TIMESTAMPTZ     DEFAULT NOW(),
    col_updated_at    TIMESTAMPTZ     DEFAULT NOW()
);


-- =============================================================
-- POSTGIS / SPATIAL TABLE
-- Exercises geometry and geography with explicit and generic types.
-- NOTE: PostGIS support in the tool is currently limited — this
-- table is intentionally included to expose that gap.
-- =============================================================
CREATE TABLE spatial_features (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    geom_point      geometry(Point, 4326),
    geom_linestring geometry(LineString, 4326),
    geog_polygon    geography(Polygon, 4326),
    geom_generic    geometry,
    geog_generic    geography,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);


-- =============================================================
-- HSTORE TABLE
-- =============================================================
CREATE TABLE key_value_store (
    id          BIGSERIAL PRIMARY KEY,
    entity_type TEXT   NOT NULL,
    entity_id   BIGINT NOT NULL,
    attributes  HSTORE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);


-- =============================================================
-- AUTO-INCREMENT PATTERNS
-- =============================================================

-- SMALLSERIAL / SERIAL / BIGSERIAL
CREATE TABLE serial_examples (
    id_smallserial SMALLSERIAL PRIMARY KEY,
    id_serial      SERIAL,
    id_bigserial   BIGSERIAL,
    label          TEXT
);

-- GENERATED ALWAYS AS IDENTITY / GENERATED BY DEFAULT AS IDENTITY
CREATE TABLE identity_examples (
    id_always  INTEGER GENERATED ALWAYS AS IDENTITY
                   (START WITH 100 INCREMENT BY 2)   PRIMARY KEY,
    id_default INTEGER GENERATED BY DEFAULT AS IDENTITY
                   (START WITH 500 INCREMENT BY 10),
    label      TEXT
);

-- Standalone sequence referenced via DEFAULT nextval(...)
CREATE TABLE custom_seq_table (
    id    BIGINT DEFAULT nextval('custom_id_seq') PRIMARY KEY,
    label TEXT
);


-- =============================================================
-- RELATIONSHIP PATTERNS
-- =============================================================

-- One-to-many root
CREATE TABLE parents (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- One-to-many child; includes a stored generated column (PG 12+)
CREATE TABLE children (
    id         BIGSERIAL PRIMARY KEY,
    parent_id  BIGINT NOT NULL REFERENCES parents (id) ON DELETE CASCADE,
    name       TEXT NOT NULL,
    birth_date DATE,
    birth_year SMALLINT GENERATED ALWAYS AS
                   (EXTRACT(YEAR FROM birth_date)::SMALLINT) STORED,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- One-to-one (unique FK)
CREATE TABLE profiles (
    id         BIGSERIAL PRIMARY KEY,
    parent_id  BIGINT NOT NULL UNIQUE REFERENCES parents (id) ON DELETE CASCADE,
    bio        TEXT,
    avatar_url TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Many-to-many via join table
CREATE TABLE tags (
    id   BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT NOT NULL,
    CONSTRAINT uq_tags_name_slug UNIQUE (name, slug)
);

CREATE TABLE parent_tags (
    parent_id BIGINT NOT NULL REFERENCES parents (id) ON DELETE CASCADE,
    tag_id    BIGINT NOT NULL REFERENCES tags (id)    ON DELETE CASCADE,
    PRIMARY KEY (parent_id, tag_id)
);

-- Self-referential FK
CREATE TABLE categories (
    id        BIGSERIAL PRIMARY KEY,
    parent_id BIGINT REFERENCES categories (id) ON DELETE SET NULL,
    name      TEXT    NOT NULL,
    depth     INTEGER NOT NULL DEFAULT 0 CHECK (depth >= 0)
);

-- Circular FK with DEFERRABLE INITIALLY DEFERRED
-- Required pattern for the restore tool: both FKs must exist but
-- rows can only be inserted inside a deferred transaction.
CREATE TABLE node_a (
    id        BIGSERIAL PRIMARY KEY,
    label     TEXT   NOT NULL,
    node_b_id BIGINT
);

CREATE TABLE node_b (
    id        BIGSERIAL PRIMARY KEY,
    label     TEXT   NOT NULL,
    node_a_id BIGINT
);

ALTER TABLE node_a
    ADD CONSTRAINT fk_node_a_to_b
    FOREIGN KEY (node_b_id) REFERENCES node_b (id)
    DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE node_b
    ADD CONSTRAINT fk_node_b_to_a
    FOREIGN KEY (node_a_id) REFERENCES node_a (id)
    DEFERRABLE INITIALLY DEFERRED;


-- =============================================================
-- PARTITIONED TABLES
-- NOTE: Partitioned tables are NOT yet supported by the restore
-- tool. These are intentionally included to make that gap visible
-- so test failures are expected, not surprising.
-- =============================================================

-- Range-partitioned by date
CREATE TABLE events (
    id         BIGINT GENERATED ALWAYS AS IDENTITY,
    event_date DATE   NOT NULL,
    title      TEXT   NOT NULL,
    region     region_enum,
    payload    JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, event_date)
) PARTITION BY RANGE (event_date);

CREATE TABLE events_2023    PARTITION OF events FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE events_2024    PARTITION OF events FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE events_2025    PARTITION OF events FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE events_default PARTITION OF events DEFAULT;

-- List-partitioned by region text
CREATE TABLE regional_data (
    id          BIGINT GENERATED ALWAYS AS IDENTITY,
    region      TEXT   NOT NULL,
    payload     JSONB,
    recorded_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, region)
) PARTITION BY LIST (region);

CREATE TABLE regional_data_north PARTITION OF regional_data FOR VALUES IN ('north');
CREATE TABLE regional_data_south PARTITION OF regional_data FOR VALUES IN ('south');
CREATE TABLE regional_data_east  PARTITION OF regional_data FOR VALUES IN ('east');
CREATE TABLE regional_data_west  PARTITION OF regional_data FOR VALUES IN ('west');
CREATE TABLE regional_data_other PARTITION OF regional_data DEFAULT;

-- Hash-partitioned by id
CREATE TABLE sharded_records (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payload    TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
) PARTITION BY HASH (id);

CREATE TABLE sharded_records_0 PARTITION OF sharded_records FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE sharded_records_1 PARTITION OF sharded_records FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE sharded_records_2 PARTITION OF sharded_records FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE sharded_records_3 PARTITION OF sharded_records FOR VALUES WITH (MODULUS 4, REMAINDER 3);

-- Sub-partitioned: range by timestamp → hash by id
CREATE TABLE sub_partitioned (
    id         BIGINT      GENERATED ALWAYS AS IDENTITY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload    TEXT,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE sub_partitioned_2024 PARTITION OF sub_partitioned
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')
    PARTITION BY HASH (id);

CREATE TABLE sub_partitioned_2025 PARTITION OF sub_partitioned
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')
    PARTITION BY HASH (id);

CREATE TABLE sub_partitioned_2024_0 PARTITION OF sub_partitioned_2024 FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE sub_partitioned_2024_1 PARTITION OF sub_partitioned_2024 FOR VALUES WITH (MODULUS 2, REMAINDER 1);
CREATE TABLE sub_partitioned_2025_0 PARTITION OF sub_partitioned_2025 FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE sub_partitioned_2025_1 PARTITION OF sub_partitioned_2025 FOR VALUES WITH (MODULUS 2, REMAINDER 1);


-- =============================================================
-- EXCLUSION CONSTRAINT TABLE
-- Uses btree_gist to support `=` on BIGINT inside a GiST index.
-- Prevents overlapping bookings per parent.
-- =============================================================
CREATE TABLE parent_bookings (
    id        BIGSERIAL PRIMARY KEY,
    parent_id BIGINT    NOT NULL REFERENCES parents (id) ON DELETE CASCADE,
    during    DATERANGE NOT NULL,
    EXCLUDE USING GIST (parent_id WITH =, during WITH &&)
);


-- =============================================================
-- UNLOGGED TABLE
-- Available on AWS RDS. Not crash-safe; useful for ephemeral data.
-- =============================================================
CREATE UNLOGGED TABLE session_cache (
    session_id UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_data  JSONB,
    expires_at TIMESTAMPTZ NOT NULL
);


-- =============================================================
-- AUDIT LOG TABLE (created before trigger functions reference it)
-- =============================================================
CREATE TABLE audit_log (
    id         BIGSERIAL   PRIMARY KEY,
    table_name TEXT        NOT NULL,
    record_id  BIGINT,
    action     TEXT        NOT NULL,
    changed_at TIMESTAMPTZ DEFAULT NOW()
);


-- =============================================================
-- INDEXES
-- =============================================================

-- B-tree (default)
CREATE INDEX idx_children_parent_id   ON children    (parent_id);
CREATE INDEX idx_parents_name         ON parents     (name);
CREATE INDEX idx_categories_parent_id ON categories  (parent_id);

-- Hash
CREATE INDEX idx_scalar_uuid_hash ON all_scalar_types USING HASH (col_uuid);

-- GIN on JSONB
CREATE INDEX idx_scalar_jsonb_gin ON all_scalar_types USING GIN (col_jsonb);
-- GIN on integer array
CREATE INDEX idx_scalar_int_array_gin ON all_scalar_types USING GIN (col_int_array);
-- GIN with pg_trgm for text similarity searches
CREATE INDEX idx_parents_name_trgm ON parents USING GIN (name gin_trgm_ops);

-- GiST on geometry column
CREATE INDEX idx_spatial_geom_gist ON spatial_features USING GIST (geom_generic);
-- GiST on range column
CREATE INDEX idx_scalar_daterange_gist ON all_scalar_types USING GIST (col_daterange);

-- SP-GiST on INET (native inet_spgist opclass)
CREATE INDEX idx_scalar_inet_spgist ON all_scalar_types USING SPGIST (col_inet);

-- BRIN on time-ordered partitioned table (append-only access pattern)
CREATE INDEX idx_events_created_brin ON events USING BRIN (created_at);

-- Partial index
CREATE INDEX idx_children_has_birthdate ON children (parent_id)
    WHERE birth_date IS NOT NULL;

-- Expression index
CREATE INDEX idx_tags_slug_lower ON tags (LOWER(slug));


-- =============================================================
-- VIEWS
-- =============================================================
CREATE VIEW children_with_parents AS
SELECT
    c.id         AS child_id,
    c.name       AS child_name,
    c.birth_date,
    c.birth_year,
    p.id         AS parent_id,
    p.name       AS parent_name
FROM children c
JOIN parents p ON p.id = c.parent_id;


-- =============================================================
-- MATERIALIZED VIEW
-- UNIQUE index on parent_id enables REFRESH CONCURRENTLY.
-- =============================================================
CREATE MATERIALIZED VIEW parent_tag_counts AS
SELECT
    p.id             AS parent_id,
    p.name           AS parent_name,
    COUNT(pt.tag_id) AS tag_count
FROM parents p
LEFT JOIN parent_tags pt ON pt.parent_id = p.id
GROUP BY p.id, p.name;

CREATE UNIQUE INDEX idx_parent_tag_counts_pk
    ON parent_tag_counts (parent_id);


-- =============================================================
-- TRIGGER FUNCTIONS
-- =============================================================

-- Generic updated_at maintenance (reusable on any table with updated_at column)
CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

-- Audit logger for parents table updates
CREATE OR REPLACE FUNCTION fn_audit_parent_update()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO audit_log (table_name, record_id, action)
    VALUES ('parents', NEW.id, 'UPDATE');
    RETURN NEW;
END;
$$;


-- =============================================================
-- TRIGGERS
-- =============================================================

-- BEFORE INSERT OR UPDATE: maintain updated_at on children
CREATE TRIGGER trg_children_set_updated_at
    BEFORE INSERT OR UPDATE ON children
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- AFTER UPDATE: write audit record on parent changes
CREATE TRIGGER trg_parents_audit_update
    AFTER UPDATE ON parents
    FOR EACH ROW EXECUTE FUNCTION fn_audit_parent_update();


-- =============================================================
-- SCALAR FUNCTION
-- =============================================================
CREATE OR REPLACE FUNCTION fn_age_in_years(p_birth_date DATE)
RETURNS INTEGER LANGUAGE sql STABLE AS $$
    SELECT EXTRACT(YEAR FROM AGE(p_birth_date))::INTEGER;
$$;


-- =============================================================
-- STORED PROCEDURE
-- =============================================================
CREATE OR REPLACE PROCEDURE proc_expire_sessions(p_cutoff TIMESTAMPTZ)
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM session_cache WHERE expires_at < p_cutoff;
END;
$$;


-- =============================================================
-- TABLE INHERITANCE (classical PostgreSQL, not declarative partitioning)
-- NOTE: Table inheritance interacts with the restore tool's
-- table ordering logic — included here to expose any gaps.
-- =============================================================
CREATE TABLE base_entity (
    id         BIGSERIAL   PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    is_active  BOOLEAN     DEFAULT TRUE
);

CREATE TABLE entity_type_a (
    label    TEXT           NOT NULL,
    priority priority_level DEFAULT 'medium'
) INHERITS (base_entity);

CREATE TABLE entity_type_b (
    score NUMERIC(10, 4),
    tags  TEXT[]
) INHERITS (base_entity);


COMMIT;
