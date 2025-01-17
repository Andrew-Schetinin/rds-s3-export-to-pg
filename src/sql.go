package main

const findIndexes = "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = $1 ORDER BY indexname"

const findConstrains = `
            SELECT conname, pg_get_constraintdef(oid) AS definition
            FROM pg_constraint
            WHERE conrelid = (SELECT oid FROM pg_class WHERE relname = $1)
            ORDER BY conname, definition
        `

const dropConstraint = "ALTER TABLE %s DROP CONSTRAINT %s;"

const addConstraint = "ALTER TABLE %s ADD CONSTRAINT %s %s;"

const dropIndex = "DROP INDEX IF EXISTS %s;"

const listTables = `
	SELECT table_schema || '.' || table_name AS name  FROM information_schema.tables
	WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND table_type NOT IN ('VIEW')
	ORDER BY table_schema, table_name
	`

const listFKeys = `
	SELECT c.conname                                 AS constraint_name,
       c.contype                                     AS constraint_type,
       sch.nspname                                   AS "self_schema",
       tbl.relname                                   AS "self_table",
       STRING_AGG(col.attname, ',') AS "self_columns",
       f_sch.nspname                                 AS "foreign_schema",
       f_tbl.relname                                 AS "foreign_table",
       STRING_AGG(f_col.attname, ',') AS "foreign_columns",
       pg_get_constraintdef(c.oid)                   AS definition
	FROM pg_constraint c
         LEFT JOIN LATERAL UNNEST(c.conkey) WITH ORDINALITY AS u(attnum, attposition) ON TRUE
         LEFT JOIN LATERAL UNNEST(c.confkey) WITH ORDINALITY AS f_u(attnum, attposition) ON f_u.attposition = u.attposition
         JOIN pg_class tbl ON tbl.oid = c.conrelid
         JOIN pg_namespace sch ON sch.oid = tbl.relnamespace
         LEFT JOIN pg_attribute col ON (col.attrelid = tbl.oid AND col.attnum = u.attnum)
         LEFT JOIN pg_class f_tbl ON f_tbl.oid = c.confrelid
         LEFT JOIN pg_namespace f_sch ON f_sch.oid = f_tbl.relnamespace
         LEFT JOIN pg_attribute f_col ON (f_col.attrelid = f_tbl.oid AND f_col.attnum = f_u.attnum)
	WHERE sch.nspname NOT IN ('pg_catalog')
	GROUP BY constraint_name, constraint_type, "self_schema", "self_table", definition, "foreign_schema", "foreign_table"
	ORDER BY "self_schema", "self_table";
	`

const selectTableSize = "SELECT COUNT(*) FROM %s"

const disableTriggers = "ALTER TABLE %s DISABLE TRIGGER ALL;"

const enableTriggers = "ALTER TABLE %s ENABLE TRIGGER ALL;"

const deferConstraints = "SET CONSTRAINTS ALL DEFERRED;"
