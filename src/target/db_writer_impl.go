package target

import (
	"context"
	"dbrestore/source"
	"dbrestore/utils"
	"fmt"
	"go.uber.org/zap"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// WriteTable writes data to a database table using the provided source and field mapper for mapping fields.
func (w *DbWriter) WriteTable(source source.Source, mapper *FieldMapper) (ret int, err error) {
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

// writeTableData writes data from a source into table parts based on a field mapper, processing files in grouped subfolders.
// It verifies the presence of success marker files in each subfolder before processing Parquet files and skips unsupported files.
// Returns the total size of written data or an error if processing fails.
func (w *DbWriter) writeTableData(source source.Source, mapper *FieldMapper) (ret int, err error) {
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

// writeTablePart processes a Parquet file and writes its data to a database table using either CSV or binary protocols.
// It validates the table size before and after the operation to ensure data consistency.
// Returns the number of rows written and an error if any issues occur during the process.
func (w *DbWriter) writeTablePart(src source.Source, mapper *FieldMapper, relativePath string) (ret int, err error) {
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
