package main

import (
	"fmt"
	"github.com/parquet-go/parquet-go"
	"go.uber.org/zap"
	"io"
	"os"
)

type ParquetReader struct {
	fileInfo    FileInfo
	mapper      *FieldMapper
	isOpen      bool
	wasClosed   bool
	lastError   error
	file        *os.File
	parquetFile *parquet.File
	rowCount    int64
	channel     chan NextRow
	nextRow     []any
	rowCounter  int64
}

type NextRow struct {
	row []any
	err error
}

// Next attempts to establish or maintain the reader's state, returning true if no error occurs and false otherwise.
// It implements the interface pgx.CopyFromSource
func (r *ParquetReader) Next() bool {
	if r.lastError == nil {
		if !r.isOpen && !r.wasClosed {
			r.lastError = r.Open(r.fileInfo)
			if r.lastError == nil {
				count, err := r.Read(r.mapper)
				logger.Debug("ParquetReader.Next(): r.Read()", zap.Int("count", count), zap.Error(err))
				r.lastError = err
			}
		}
	}
	if r.lastError != nil {
		return false
	}
	data, ok := <-r.channel
	if !ok {
		r.lastError = io.EOF
		return false
	}
	if data.err != nil {
		r.lastError = data.err
		return false
	}
	r.nextRow = data.row
	r.rowCounter++
	return true
}

// Values returns all values from the current row or an error if one occurred during the read process.
// It implements the interface pgx.CopyFromSource
func (r *ParquetReader) Values() ([]any, error) {
	if r.lastError != nil {
		return nil, r.lastError
	}
	return r.nextRow, nil
}

// Err returns the last error encountered by the ParquetReader, or nil if no error has occurred.
// It implements the interface pgx.CopyFromSource
func (r *ParquetReader) Err() error {
	return r.lastError
}

// Open initializes the ParquetReader with the specified FileInfo and opens the associated Parquet file for reading.
func (r *ParquetReader) Open(fileInfo FileInfo) error {
	if r.isOpen || r.wasClosed {
		return fmt.Errorf("the input file ParquetReader had been already open")
	}
	r.fileInfo = fileInfo

	// Open the Parquet file
	fileName := fileInfo.localPath
	osFile, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", fileName, err)
	}
	r.file = osFile
	r.isOpen = true

	fileStat, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info for %s: %w", fileName, err)
	}
	size := fileStat.Size()
	f, err := parquet.OpenFile(r.file, size)
	if err != nil {
		return fmt.Errorf("failed to open the file %s: %w", fileName, err)
	}
	r.parquetFile = f
	r.rowCount = f.NumRows()
	logger.Debug(fmt.Sprintf(`Row count = %d`, r.rowCount))

	return nil
}

// Close releases the resources held by the ParquetReader and closes the associated file if it is currently open.
func (r *ParquetReader) Close() (err error) {
	if r.isOpen {
		r.isOpen = false
		r.wasClosed = true
		err = r.file.Close()
		r.file = nil
	}
	return
}

// Read reads rows from a parquet file using a FieldMapper and starts a goroutine to process rows asynchronously.
func (r *ParquetReader) Read(mapper *FieldMapper) (int, error) {
	logger.Debug("f.Schema(): ", zap.String("name", r.parquetFile.Schema().Name()))
	for i, column := range r.parquetFile.Schema().Columns() {
		for j, path := range column {
			logger.Debug("Column", zap.Int("i", i), zap.Int("j", j), zap.String("localPath", path))
		}
	}

	for i, rowGroup := range r.parquetFile.RowGroups() {
		logger.Debug("RowGroup: ", zap.Int("index", i))
		for j, columnChunk := range rowGroup.ColumnChunks() {
			logger.Debug("ColumnChunk: ", zap.Int("index", j), zap.Int("column", columnChunk.Column()),
				zap.Any("type", columnChunk.Type()))
		}
	}

	r.channel = make(chan NextRow)

	go func() {
		defer func(r *ParquetReader) {
			err := r.Close()
			if err != nil {
				logger.Error("ERROR: ", zap.Error(err))
			}
		}(r)

		for _, rowGroup := range r.parquetFile.RowGroups() {
			rowReader := rowGroup.Rows()
			for {
				row := make([]parquet.Row, 1)
				rowCount, err := rowReader.ReadRows(row)
				if err != nil {
					if err == io.EOF {
						break
					}
					logger.Error("Error reading row", zap.Error(err))
					break
				}

				if rowCount != 1 {
					err = fmt.Errorf("the row count is not 1")
				}

				singleRow := row[0]
				logger.Debug("singleRow", zap.Any("singleRow", singleRow))

				var rowData = NextRow{
					row: make([]any, len(singleRow)),
					err: err,
				}
				for i, x := range singleRow {
					rowData.row[i], err = mapper.transform(x)
					if err != nil {
						logger.Error("Error transforming row", zap.Int("index", i),
							zap.Any("value", x), zap.Any("row", row), zap.Error(err))
						close(r.channel)
						return
					}
				}

				r.channel <- rowData

				logger.Debug("Row", zap.Any("row", row), zap.Int64("rowCounter", r.rowCounter),
					zap.Int("rowCount", rowCount))
				// Process the row as needed
			}
		}

		close(r.channel)
	}()

	return int(r.rowCount), nil
}

//func readParquet(localFilePath string) {
//
//	// Open the Parquet file
//	fileName := localFilePath // "/Users/andrews/Downloads/part-00000-d68fd39b-0c1b-401b-aeb2-ee1f8ded89dc-c000.gz.parquet"
//	//"/Users/andrews/Downloads/flights-1m.parquet" // "/Users/andrews/Downloads/mtcars.parquet"
//	file, err := os.Open(fileName)
//	if err != nil {
//		logger.Error("Failed to open file: "+fileName, zap.Error(err))
//	}
//	defer func(file *os.File) {
//		err := file.Close()
//		if err != nil {
//			logger.Error("ERROR: ", zap.Error(err))
//		}
//	}(file)
//
//	var fileInfo, err2 = file.Stat()
//	if err2 != nil {
//		logger.Error("Failed to get file info: "+fileName, zap.Error(err2))
//		return
//	}
//	size := fileInfo.Size()
//	f, err := parquet.OpenFile(file, size)
//	if err != nil {
//		logger.Error("Failed to open file: "+fileName, zap.Error(err))
//	}
//	rowCount := f.NumRows()
//	logger.Debug(fmt.Sprintf(`Row count = %d`, rowCount))
//
//	logger.Debug("f.Schema(): ", zap.String("name", f.Schema().Name()))
//	for i, column := range f.Schema().Columns() {
//		for j, path := range column {
//			logger.Debug("Column", zap.Int("i", i), zap.Int("j", j), zap.String("localPath", path))
//		}
//	}
//
//	for i, rowGroup := range f.RowGroups() {
//		logger.Debug("RowGroup: ", zap.Int("index", i))
//		for j, columnChunk := range rowGroup.ColumnChunks() {
//			logger.Debug("ColumnChunk: ", zap.Int("index", j))
//			columnChunk.Column()
//		}
//	}
//
//	//f, err := parquet.ReadFile(file, size)
//	//if err != nil {
//	//	...
//	//}
//
//	//// Create a parquet.File from the os.File
//	//pFile, err := parquet.NewP.NewNewParquetFile(file)
//	//if err != nil {
//	//	// Handle the error
//	//}
//	//defer pFile.Close()
//	//
//	//// Create a new Parquet reader
//	//pqReader, err := reader.NewParquetReader(file, new(User), 4) // 4 goroutines
//	//if err != nil {
//	//	log.Fatalf("Failed to create Parquet reader: %v", err)
//	//}
//	//defer pqReader.ReadStop() // Ensure the reader stops after execution
//	//
//	//// Read data in batches of 1000 records
//	//batchSize := 1000
//	//totalRows := int(pqReader.GetNumRows())
//	//
//	//fmt.Printf("Found %d rows in the Parquet file\n", totalRows)
//	//
//	//batch := make([]User, batchSize) // Allocate a slice for the batch
//	//for i := 0; i < totalRows; i += batchSize {
//	//	// Calculate the size of the slice to read (batchSize or remaining rows)
//	//	rowsToRead := batchSize
//	//	if i+batchSize > totalRows {
//	//		rowsToRead = totalRows - i
//	//		batch = make([]User, rowsToRead) // Resize for the final batch
//	//	}
//	//
//	//	// Read the batch into the pre-allocated slice
//	//	if err := pqReader.Read(&batch); err != nil {
//	//		log.Fatalf("Failed to read batch at index %d: %v", i, err)
//	//	}
//	//
//	//	// Process the batch
//	//	fmt.Printf("Processing batch starting at row %d\n", i)
//	//	for _, user := range batch {
//	//		fmt.Printf("User: Name=%s, Age=%d\n", user.Name, user.Age)
//	//	}
//	//}
//
//}
