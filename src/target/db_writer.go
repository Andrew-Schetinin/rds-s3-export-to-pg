package target

import (
	"bytes"
	"context"
	"dbrestore/config"
	"dbrestore/source"
	"dbrestore/utils"
	"fmt"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"regexp"
)

// DbWriter represents a utility for writing data to a database through a specified connection string.
type DbWriter struct {

	// ConnectionString connection string in the format
	// connStr := "postgres://andrews:asd@localhost:5432/test?sslmode=disable"
	ConnectionString string

	// db the database connection (opened by this class)
	db *pgx.Conn

	// regExPrimary holds the compiled regular expression used for primary keys pattern matching.
	regExPrimary *regexp.Regexp

	// regExIdx represents a compiled regular expression used for pattern matching of indexes.
	regExIdx *regexp.Regexp

	// regExCon is a compiled regular expression used for pattern matching operations of constraints.
	regExCon *regexp.Regexp
}

// NewDatabaseWriter creates and initializes a new DbWriter instance with the provided connection details and regex patterns.
func NewDatabaseWriter(host string, port int, name string, user string, password string, mode bool) DbWriter {
	// Compile the regular expression
	rePrimary, err := regexp.Compile(".*PRIMARY KEY.*")
	if err != nil {
		log.Error("ERROR: ", zap.Error(err))
	}
	reIdx, err := regexp.Compile(".*UNIQUE INDEX.*(id).*")
	if err != nil {
		log.Error("ERROR: ", zap.Error(err))
	}
	reCon, err := regexp.Compile(".*UNIQUE.*")
	if err != nil {
		log.Error("ERROR: ", zap.Error(err))
	}
	return DbWriter{
		ConnectionString: fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			user,
			password,
			host,
			port,
			name,
			map[bool]string{true: "require", false: "disable"}[mode],
		),
		regExPrimary: rePrimary,
		regExIdx:     reIdx,
		regExCon:     reCon,
	}
}

// Connect establishes a connection to the database using the provided connection string in the DbWriter instance.
func (w *DbWriter) Connect() error {
	log.Debug("Connecting to the database")
	db, err := pgx.Connect(context.Background(), w.ConnectionString)
	if err == nil && db == nil {
		return fmt.Errorf("database connection is nil")
	}
	w.db = db
	return err
}

// Close closes the database connection held by the DbWriter and logs an error if the closure fails.
func (w *DbWriter) Close() {
	if w.db != nil {
		log.Debug("Closing the database connection")
		err := w.db.Close(context.Background())
		w.db = nil
		if err != nil {
			log.Error("ERROR: ", zap.Error(err))
		}
	}
}

// closeTransactionInPanic ensures proper handling of a transaction in case of a panic by performing a rollback.
func closeTransactionInPanic(tx pgx.Tx) {
	log.Debug("Closing the transaction")
	if p := recover(); p != nil {
		log.Debug("Rollback on panic")
		err := tx.Rollback(context.Background())
		if err != nil {
			log.Warn("Rollback error during panic", zap.Error(err))
		}
	}
}

// GetTablesOrdered retrieves a list of database tables ordered by their creation dependencies.
func (w *DbWriter) GetTablesOrdered() (ret []string, err error) {
	log.Debug("Getting ordered tables...")

	// this retrieves only the FK between tables, so some tables are missing
	fkMap, err := w.getFKeys()
	if err != nil {
		return
	}

	if !fkMap.IsAcyclic() {
		return nil, fmt.Errorf("graph is not acyclic - cannot continue processing")
	}

	// sort in order of FK dependencies
	ret = fkMap.TopologicalSort()
	log.Debug("Tables sorted", zap.Int("table count", len(ret)))

	// Get a full list of tables, because we want to process all of them
	tables, err := w.getTables()
	if err != nil {
		return
	}
	log.Debug("Tables retrieved from the database", zap.Int("table count", len(tables)))

	// Create a set from the sorted tables list - we need it for verifying which tables are missing
	setTablesFK := make(map[string]struct{}, len(ret)) // Create a set
	for _, tableName := range ret {
		setTablesFK[tableName] = struct{}{}
	}

	// append all missing tables to the sorted list
	for _, tableName := range tables {
		if _, exists := setTablesFK[tableName]; !exists {
			ret = append(ret, tableName)
		}
	}

	if len(ret) != len(tables) {
		return nil, fmt.Errorf("table count mismatch: sortedTables.len = %d, tables.len = %d",
			len(ret), len(tables))
	}

	// report to the log the order of the tables
	for _, tableName := range ret {
		children := fkMap.GetNodeChildren(tableName)
		s := ""
		if children != nil {
			for key := range *children {
				s += key + " "
			}
		}
		log.Debug("Ordered table: ", zap.String("table", tableName), zap.String("children", s))
	}

	// Create a map from table names to their indices
	tableIndexMap := make(map[string]int, len(ret))
	for index, tableName := range ret {
		tableIndexMap[tableName] = index
	}

	errorCount := 0
	for _, index := range fkMap.Graph {
		node := fkMap.Nodes[index]
		// Check if the table exists in tableIndexMap
		if parentIndex, exists := tableIndexMap[node.Name]; exists {
			for dependentTableName := range node.Children {
				// Check if the dependent table exists in tableIndexMap
				if dependentIndex, exists := tableIndexMap[dependentTableName]; exists {
					// self-references are permitted
					if parentIndex <= dependentIndex && node.Name != dependentTableName {
						errorCount += 1
						log.Error("Parent table index is not larger than dependent table index",
							zap.String("parent_table", node.Name),
							zap.String("dependent_table", dependentTableName),
							zap.Int("parent_index", parentIndex),
							zap.Int("dependent_index", dependentIndex),
						)
					}
				} else {
					log.Warn("Dependent table not found in tableIndexMap",
						zap.String("dependent_table", dependentTableName),
					)
				}
			}
		} else {
			log.Warn("Parent table not found in tableIndexMap",
				zap.String("parent_table", node.Name),
			)
		}
	}
	if errorCount > 0 {
		return nil, fmt.Errorf("table order validation failed. error_count: %d", errorCount)
	}

	return
}

// GetFieldMapper creates and returns a FieldMapper instance using the provided ParquetFileInfo and config settings.
func (w *DbWriter) GetFieldMapper(info source.ParquetFileInfo, config *config.Config) (ret FieldMapper, err error) {
	mapper := FieldMapper{
		Info:   info,
		Writer: w,
		Config: config,
	}
	return mapper, nil
}

// getTableSize retrieves the size of a database table by its name and returns it as an integer value.
// Returns -1 if an error occurs or the table size cannot be determined.
func (w *DbWriter) getTableSize(tableName string) int {
	size := -1
	query := fmt.Sprintf(selectTableSize, utils.SanitizeTableName(tableName))
	err := w.db.QueryRow(context.Background(), query).Scan(&size)
	if err != nil {
		log.Error("Failed to fetch table size", zap.String("table_name", tableName), zap.Error(err))
		return -1
	}
	return size
}

// copyFromBinary writes data to a database table using binary format from a Parquet source through a field mapper configuration.
// It returns the number of rows written and an error if the operation fails.
func (w *DbWriter) copyFromBinary(mapper *FieldMapper, copyFromSource *source.ParquetReader) (ret int64, err error) {
	ret, err = w.db.CopyFrom(
		context.Background(),
		utils.CreatePgxIdentifier(mapper.Info.TableName),
		mapper.getFieldNames(), //[]string{"first_name", "last_name", "age"},
		copyFromSource,         // pgx.CopyFromRows(rows),
	)
	return
}

// copyFromCSV copies data from a ParquetReader source to a PostgreSQL database table using the COPY command.
// The FieldMapper maps the source fields to the target table's columns.
// Returns the number of rows copied and an error, if any.
func (w *DbWriter) copyFromCSV(mapper *FieldMapper, copyFromSource *source.ParquetReader) (ret int64, err error) {
	pgConn := w.db.PgConn()

	quotedTableName := utils.CreatePgxIdentifier(mapper.Info.TableName).Sanitize()
	buf := &bytes.Buffer{}
	for i, cn := range mapper.Info.Columns {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(utils.CreatePgxIdentifier(cn.ColumnName).Sanitize())
	}
	quotedColumnNames := buf.String()

	sqlQuery := fmt.Sprintf(copyTableFromCSV, quotedTableName, quotedColumnNames)

	csvReader, err := utils.ConvertToCSVReader(context.Background(), copyFromSource)
	if err != nil {
		return 0, fmt.Errorf("failed to create a CSV reader: %w", err)
	}

	from, err := pgConn.CopyFrom(context.Background(), csvReader, sqlQuery)
	if err != nil {
		return 0, fmt.Errorf("failed to execute '%s': %w", sqlQuery, err)
	}

	log.Info("Copying from CSV", zap.Int64("rows_copied", from.RowsAffected()),
		zap.String("message", from.String()), zap.Bool("insert", from.Insert()),
		zap.Bool("update", from.Update()), zap.Bool("delete", from.Delete()),
		zap.Bool("select", from.Select()))

	ret = from.RowsAffected()
	return
}

// TruncateAllTables truncates the specified tables in reverse order if they are not empty and returns the count of truncated tables.
func (w *DbWriter) TruncateAllTables(tables []string) (truncatedCount int, err error) {
	for i := len(tables) - 1; i >= 0; i-- {
		table := tables[i]
		// Query to check if the table is not empty
		query := fmt.Sprintf(checkIfTableIsNotEmpty, utils.SanitizeTableName(table))
		var tableNotEmpty bool
		err = w.db.QueryRow(context.Background(), query).Scan(&tableNotEmpty)
		if err != nil {
			return truncatedCount, fmt.Errorf("checking if table '%s' is not empty failed: %w", table, err)
		}
		if tableNotEmpty {
			log.Info("Truncating table", zap.String("table", table))
			_, err = w.db.Exec(context.Background(), fmt.Sprintf(truncateTable, utils.SanitizeTableName(table)))
			if err != nil {
				return truncatedCount, fmt.Errorf("truncating table '%s' failed: %w", table, err)
			}
			truncatedCount++
		}
	}
	return truncatedCount, nil
}
