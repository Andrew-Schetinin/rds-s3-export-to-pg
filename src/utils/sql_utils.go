package utils

import (
	"fmt"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"strings"
)

// CreatePgxIdentifier constructs pgx.Identifier out of a table name, optionally including schema.
// The input string can be SCHEMA.TABLE or TABLE (no matter the letter case).
// A wrong input string with more than one "." symbol will report an error to the log and return
// the whole input string to be wrapped as a single name,
// usually resulting in a wrong identifier that will fail the SQL query.
func CreatePgxIdentifier(tableNameWithOrWithoutSchema string) pgx.Identifier {
	s := tableNameWithOrWithoutSchema
	if strings.Contains(s, ".") {
		parts := strings.Split(s, ".")
		if len(parts) != 2 {
			// Handle the error if the identifier format is invalid (e.g., missing schema or table name)
			Logger.Error("Invalid identifier format. Expected 'schema_name.table_name'",
				zap.String("tableName", s))
		} else {
			return pgx.Identifier{parts[0], parts[1]}
		}
	}
	return pgx.Identifier{s}
}

// SanitizeTableName sanitizes a table name, optionally including schema, ensuring the format is valid for SQL queries.
// The input string SCHEMA.TABLE will be returned as "SCHEMA"."TABLE",
// and the input string "TABLE" will be returned as "TABLE".
// A wrong input string with more than one "." symbol will report an error to the log and return the input string as-is.
func SanitizeTableName(tableNameWithOrWithoutSchema string) string {
	s := tableNameWithOrWithoutSchema
	if strings.Contains(s, ".") {
		parts := strings.Split(s, ".")
		if len(parts) != 2 {
			// Handle the error if the identifier format is invalid (e.g., missing schema or table name)
			Logger.Error("Invalid identifier format. Expected 'schema_name.table_name'",
				zap.String("tableName", s))
		} else {
			identifier := pgx.Identifier{parts[0], parts[1]}
			return identifier.Sanitize() // Format the identifier
		}
	}
	identifier := pgx.Identifier{s}
	return identifier.Sanitize() // Format the identifier
}

// SplitFullTableName splits a full table name into its schema and table components if a schema is specified.
// Returns the schema and table as separate strings.
// If no schema is included, the schema is returned as an empty string.
// Panics if the input format is invalid.
func SplitFullTableName(fullTableName string) (schema string, table string) {
	schema = ""
	table = fullTableName
	if strings.Contains(fullTableName, ".") {
		parts := strings.SplitN(fullTableName, ".", 2)
		if len(parts) == 2 {
			schema, table = parts[0], parts[1]
		} else {
			panic(fmt.Sprintf("SplitFullTableName(): unexpected table name format for: %s", fullTableName))
		}
	}
	return
}
