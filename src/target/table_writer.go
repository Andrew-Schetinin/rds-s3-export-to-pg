package target

import (
	"bytes"
	"context"
	"database/sql"
	"dbrestore/config"
	"dbrestore/dag"
	"dbrestore/source"
	"dbrestore/utils"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

// IndexInfo represents metadata about a table index.
type IndexInfo struct {
	// Name is the name of the index.
	Name string
	// Def is the definition or creation statement of the index.
	Def string
}

// ConstraintInfo represents information about a database constraint, including its name and the command to define it.
type ConstraintInfo struct {
	// Name represents the identifier of the table constraint.
	Name string
	// Command represents the SQL definition or statement used to define the table constraint.
	Command string
}

// DatabaseWriter represents a utility for writing data to a database through a specified connection string.
type DatabaseWriter struct {

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

// NewDatabaseWriter creates and initializes a new DatabaseWriter instance with the provided connection details and regex patterns.
func NewDatabaseWriter(host string, port int, name string, user string, password string, mode bool) DatabaseWriter {
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
	return DatabaseWriter{
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

type Relation struct {
	constraintName string
	constraintType string
	selfSchema     string
	selfTable      string
	selfColumns    string
	foreignSchema  string
	foreignTable   string
	foreignColumns string
	definition     string
}

func (w *DatabaseWriter) Connect() error {
	log.Debug("Connecting to the database")
	db, err := pgx.Connect(context.Background(), w.ConnectionString)
	if err == nil && db == nil {
		return fmt.Errorf("database connection is nil")
	}
	w.db = db
	return err
}

func (w *DatabaseWriter) Close() {
	if w.db != nil {
		log.Debug("Closing the database connection")
		err := w.db.Close(context.Background())
		w.db = nil
		if err != nil {
			log.Error("ERROR: ", zap.Error(err))
		}
	}
}

func (w *DatabaseWriter) getIndexList(tableName string) (ret []IndexInfo, err error) {
	//const tableName = "entity_type"
	// Query for existing indexes on a specific table
	rows, err := w.db.Query(context.Background(), findIndexes, tableName)
	if err != nil {
		log.Error("ERROR: ", zap.Error(err))
		return nil, err
	}
	defer func(rows pgx.Rows) {
		rows.Close()
	}(rows)

	var indexInfos []IndexInfo

	// Iterate over the rows and construct CREATE INDEX commands
	for rows.Next() {
		var indexName, indexDef string
		err = rows.Scan(&indexName, &indexDef)
		if err != nil {
			log.Error("ERROR: ", zap.Error(err))
			return nil, err
		}

		indexInfo := IndexInfo{
			Name: indexName,
			Def:  indexDef,
			//Command: fmt.Sprintf("CREATE INDEX %s ON your_table_name %s;", indexName, indexDef),
		}
		indexInfos = append(indexInfos, indexInfo)
	}

	if err = rows.Err(); err != nil {
		log.Error("ERROR: ", zap.Error(err))
		return nil, err
	}

	return indexInfos, nil
}

func (w *DatabaseWriter) getConstraintList(tableName string) (ret []ConstraintInfo, err error) {
	rows, err := w.db.Query(context.Background(), findConstrains, tableName)
	if err != nil {
		log.Error("ERROR: ", zap.Error(err))
		return nil, err
	}
	defer func(rows pgx.Rows) {
		rows.Close()
	}(rows)
	var constraints []ConstraintInfo
	for rows.Next() {
		var name, definition string
		err = rows.Scan(&name, &definition)
		if err != nil {
			log.Error("ERROR: ", zap.Error(err))
			return nil, err
		}

		constraints = append(constraints, ConstraintInfo{
			Name:    name,
			Command: definition,
		})
	}
	if err := rows.Err(); err != nil {
		log.Error("ERROR: ", zap.Error(err))
		return nil, err
	}
	return constraints, nil
}

func (w *DatabaseWriter) WriteTable(source source.Source, mapper *FieldMapper) (ret int, err error) {
	start := time.Now()
	tableName := mapper.Info.TableName
	indexInfos, err := w.getIndexList(tableName)
	if err != nil {
		return
	}
	constraints, err := w.getConstraintList(tableName)
	if err != nil {
		return
	}
	// Begin a transaction
	tx, err := w.db.Begin(context.Background())
	if err != nil {
		return
	}
	defer closeTransactionInPanic(tx)

	rows, err := w.db.Query(context.Background(), deferConstraints)
	if err != nil {
		_ = tx.Rollback(context.Background())
		return
	}
	log.Debug("deferConstraints query executed", zap.Any("rows", rows))
	rows.Close()

	rows, err = w.db.Query(context.Background(), fmt.Sprintf(disableTriggers, utils.SanitizeTableName(tableName)))
	if err != nil {
		_ = tx.Rollback(context.Background())
		return
	}
	log.Debug("deferConstraints query executed", zap.Any("rows", rows))
	rows.Close()

	err = w.dropIndexes(tableName, constraints, err, tx, indexInfos)
	if err != nil {
		_ = tx.Rollback(context.Background())
		return
	}
	ret, err = w.writeTableData(source, mapper)
	if err != nil {
		_ = tx.Rollback(context.Background())
		return
	}
	err = w.restoreIndexes(tableName, indexInfos, err, tx, constraints)
	if err != nil {
		_ = tx.Rollback(context.Background())
		return
	}

	rows, err = w.db.Query(context.Background(), fmt.Sprintf(enableTriggers, utils.SanitizeTableName(tableName)))
	if err != nil {
		_ = tx.Rollback(context.Background())
		return
	}
	log.Debug("deferConstraints query executed", zap.Any("rows", rows))
	rows.Close()

	err = tx.Commit(context.Background())

	recordsPerSecond := 0.0
	secondsPassed := time.Since(start).Seconds()
	if secondsPassed > 0 {
		recordsPerSecond = float64(ret) / secondsPassed
	} else if microsecondsPassed := time.Since(start).Milliseconds(); microsecondsPassed > 0 {
		x := ret * 1000000
		recordsPerSecond = float64(x) / float64(microsecondsPassed)
	}

	log.Debug("COPY TO command executed successfully",
		zap.String("table", mapper.Info.TableName),
		zap.Int("rows_copied", ret),
		zap.Duration("execution_time", time.Since(start)),
		zap.Int64("records_per_second", int64(recordsPerSecond)))

	return
}

func (w *DatabaseWriter) restoreIndexes(tableName string, indexInfos []IndexInfo, err error, tx pgx.Tx, constraints []ConstraintInfo) error {
	for _, indexInfo := range indexInfos {
		if w.regExIdx.MatchString(indexInfo.Def) {
			log.Debug("Skipping the unique index: ", zap.String("command", indexInfo.Def))
		} else {
			log.Info(indexInfo.Def)
			_, err = tx.Exec(context.Background(), indexInfo.Def)
			if err != nil {
				log.Error("ERROR: ", zap.Error(err))
				break
			}
		}
	}

	for _, constraint := range constraints {
		var createSql = fmt.Sprintf(addConstraint, utils.SanitizeTableName(tableName), utils.SanitizeTableName(constraint.Name),
			constraint.Command)
		if w.regExPrimary.MatchString(createSql) || w.regExCon.MatchString(constraint.Command) {
			log.Debug("Skipping the primary key constraint: ", zap.String("command", constraint.Command))
		} else {
			log.Info(createSql)
			_, err = tx.Exec(context.Background(), createSql)
			if err != nil {
				log.Error("ERROR: ", zap.Error(err))
				break
			}
		}
	}
	return err
}

func (w *DatabaseWriter) dropIndexes(tableName string, constraints []ConstraintInfo, err error, tx pgx.Tx, indexInfos []IndexInfo) error {
	for _, constraint := range constraints {
		var dropSql = fmt.Sprintf(dropConstraint, utils.SanitizeTableName(tableName), utils.SanitizeTableName(constraint.Name))
		if w.regExPrimary.MatchString(constraint.Command) {
			log.Debug("Skipping the primary key constraint: ", zap.String("command", constraint.Command))
		} else {
			log.Info(dropSql)
			_, err = tx.Exec(context.Background(), dropSql)
			if err != nil {
				log.Error("ERROR: ", zap.Error(err), zap.String("command", constraint.Command))
				break
			}
		}
	}

	for _, indexInfo := range indexInfos {
		var dropSql = fmt.Sprintf(dropIndex, utils.SanitizeTableName(indexInfo.Name))
		if w.regExIdx.MatchString(indexInfo.Def) {
			log.Debug("Skipping the unique index: ", zap.String("command", indexInfo.Def))
		} else {
			log.Info(dropSql)
			_, err = tx.Exec(context.Background(), dropSql)
			if err != nil {
				log.Error("ERROR: ", zap.Error(err), zap.String("command", indexInfo.Def))
				break
			}
		}
	}
	return err
}

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
func (w *DatabaseWriter) GetTablesOrdered() (ret []string, err error) {
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

func (w *DatabaseWriter) getTables() (tables []string, err error) {
	// get all tables
	startTime := time.Now() // Start measuring time
	rows, err := w.db.Query(context.Background(), listTables)
	log.Debug("listTables query executed", zap.Duration("execution_time", time.Since(startTime)))
	if err != nil {
		return nil, fmt.Errorf("querying tables failed: %w", err)
	}
	defer func() {
		rows.Close()
	}()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("getting columns failed: %w", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getting columns failed: %w", err)
	}

	//logger.Debug("Tables retrieved successfully", zap.Strings("tables", tables))
	return tables, nil
}

func (w *DatabaseWriter) getFKeys() (*dag.FKeysGraph[Relation], error) {
	// Query for foreign key constraints in all tables
	startTime := time.Now() // Start measuring time
	if w.db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}
	log.Debug("Querying foreign keys...")                    //, zap.String("query", listFKeys))
	rows, err := w.db.Query(context.Background(), listFKeys) // Execute the query
	log.Debug("listFKeys query executed", zap.Duration("execution_time", time.Since(startTime)))
	if err != nil {
		return nil, fmt.Errorf("querying foreign keys failed: %w", err)
	}
	defer func() {
		rows.Close()
	}()

	fkMap := dag.NewFKeysGraph[Relation](1000)
	count := 0
	for rows.Next() {
		count += 1
		var r Relation
		var foreignSchema, foreignTable, foreignColumns sql.NullString
		var constraintType rune
		err := rows.Scan(&r.constraintName, &constraintType, &r.selfSchema, &r.selfTable, &r.selfColumns,
			&foreignSchema, &foreignTable, &foreignColumns, &r.definition)
		if err != nil {
			return nil, fmt.Errorf("scanning foreign key rows failed: %w", err)
		}
		if foreignSchema.Valid {
			r.foreignSchema = foreignSchema.String
		}
		if foreignTable.Valid {
			r.foreignTable = foreignTable.String
		}
		if foreignColumns.Valid {
			r.foreignColumns = foreignColumns.String
		}
		r.constraintType = string(constraintType)

		if r.constraintType != "f" {
			continue // for now skip all constraints which are not foreign keys
		}

		parentName := fmt.Sprintf("%s.%s", r.selfSchema, r.selfTable)
		node := fkMap.GetNode(parentName)
		if node == nil {
			node, err = fkMap.AddNode(parentName)
			if err != nil {
				return nil, fmt.Errorf("adding node failed: %w", err)
			}
		}

		childName := fmt.Sprintf("%s.%s", r.foreignSchema, r.foreignTable)
		node.AddChild(childName, r)
	}
	log.Debug("listFKeys query", zap.Int("row count", count),
		zap.Int("nodes count", fkMap.GetNodeCount()), zap.Int("map size", fkMap.GetGraphSize()))

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating foreign key rows failed: %w", err)
	}

	// initialize in-degree values
	fkMap.CalculateInDegree()

	return &fkMap, nil
}

func (w *DatabaseWriter) GetFieldMapper(info source.ParquetFileInfo, config *config.Config) (ret FieldMapper, err error) {
	mapper := FieldMapper{
		Info:   info,
		Writer: w,
		Config: config,
	}
	return mapper, nil
}

func (w *DatabaseWriter) getTableSize(tableName string) int {
	size := -1
	query := fmt.Sprintf(selectTableSize, utils.SanitizeTableName(tableName))
	err := w.db.QueryRow(context.Background(), query).Scan(&size)
	if err != nil {
		log.Error("Failed to fetch table size", zap.String("table_name", tableName), zap.Error(err))
		return -1
	}
	return size
}

func (w *DatabaseWriter) writeTableData(source source.Source, mapper *FieldMapper) (ret int, err error) {
	if mapper.Config.SourceDatabase == "" {
		// TODO: replace the database name with a name read from the configuration
		return -1, fmt.Errorf("source database is not set")
	}
	relativePath := fmt.Sprintf("%s/%s", mapper.Config.SourceDatabase, mapper.Info.TableName)
	allFiles, err := source.ListFilesRecursively(relativePath)
	slices.Sort(allFiles)

	// Group files by their subfolders
	groupedFiles := make(map[string][]string) // map[subfolder][]files
	for _, file := range allFiles {
		subfolder := filepath.Dir(file) // Get the subfolder path
		groupedFiles[subfolder] = append(groupedFiles[subfolder], file)
	}

	// Process each group
	for subfolder, files := range groupedFiles {
		log.Debug("Processing files in subfolder", zap.String("subfolder", subfolder))

		// Ensure the files list contains the "_success" file
		successFileFound := false
		for _, file := range files {
			s := filepath.Base(file)
			if s == "_success" || s == "_SUCCESS" {
				successFileFound = true
				break
			}
		}
		if !successFileFound {
			return -1, fmt.Errorf("missing _success file in subfolder: %s", subfolder)
		}

		// Process files in the subfolder group
		for _, file := range files {
			s := filepath.Base(file)
			if s == "_success" || s == "_SUCCESS" {
				log.Debug("Skipping the _success file")
			} else if strings.HasSuffix(s, ".parquet") {
				log.Debug("Processing file", zap.String("file", file))

				// Add specific file processing logic here
				size, err := w.writeTablePart(source, mapper, file)
				if err != nil {
					return -1, fmt.Errorf("writing table part failed: %w", err)
				}
				ret += size
			} else {
				log.Warn("Skipping file with unsupported extension", zap.String("file", file))
			}
		}
	}

	return ret, nil
}

func (w *DatabaseWriter) writeTablePart(src source.Source, mapper *FieldMapper, relativePath string) (ret int, err error) {
	file := src.GetFile(relativePath)
	copyFromSource := source.NewParquetReader(file, mapper)
	if copyFromSource.IsEmpty() {
		log.Debug("Skipping empty Parquet file", zap.String("file", relativePath))
		if copyFromSource.LastError() != nil && copyFromSource.LastError() != io.EOF {
			err = fmt.Errorf("skipping empty Parquet file '%s': %w", relativePath, copyFromSource.LastError())
		}
	} else {
		var oldTableSize, newBatchCopySize, newTableSize int64
		oldTableSize = int64(w.getTableSize(mapper.Info.TableName))
		newBatchCopySize = copyFromSource.RowCount()
		log.Debug("Writing table part", zap.String("file", relativePath),
			zap.String("table", mapper.Info.TableName), zap.Int64("old_table_size", oldTableSize),
			zap.Int64("newBatchCopySize", newBatchCopySize))
		var copied int64
		if mapper.hasUserDefinedColumn() {
			// HSTORE format does not work in the binary COPY FROM protocol by some reason, so using CSV instead
			copied, err = w.copyFromCSV(mapper, copyFromSource)
		} else {
			// by default, we prefer the binary format - it is the standard format in pgx
			copied, err = w.copyFromBinary(mapper, copyFromSource)
		}
		if err != nil && err != io.EOF {
			err = fmt.Errorf("writing the table '%s' failed for %d rows: %w",
				mapper.Info.TableName, copyFromSource.RowCount(), err)
		} else {
			ret += int(copied)
			err = nil // to erase possible io.EOF
		}
		if err == nil { // validate that all rows from Parquet were written to the table
			newTableSize = int64(w.getTableSize(mapper.Info.TableName))
			if newTableSize != (oldTableSize + newBatchCopySize) {
				err = fmt.Errorf("table size mismatch: expected = %d, new actual size = %d",
					oldTableSize, newTableSize)
			}
		}
	}
	return
}

func (w *DatabaseWriter) copyFromBinary(mapper *FieldMapper, copyFromSource *source.ParquetReader) (ret int64, err error) {
	ret, err = w.db.CopyFrom(
		context.Background(),
		utils.CreatePgxIdentifier(mapper.Info.TableName),
		mapper.getFieldNames(), //[]string{"first_name", "last_name", "age"},
		copyFromSource,         // pgx.CopyFromRows(rows),
	)
	return
}

func (w *DatabaseWriter) copyFromCSV(mapper *FieldMapper, copyFromSource *source.ParquetReader) (ret int64, err error) {
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

func (w *DatabaseWriter) TruncateAllTables(tables []string) (truncatedCount int, err error) {
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
