package source

import (
	"fmt"
	"github.com/parquet-go/parquet-go"
	"go.uber.org/zap"
	"io"
	"os"
)

// ParquetReader is a structure for reading and processing Parquet files while mapping data to a defined schema.
// It implements the interface pgx.CopyFromSource for reading rows in the format supported by CopyFrom() function.
type ParquetReader struct {
	// fileInfo contains metadata and details of the file to be processed, such as its path, size, etc.
	fileInfo FileInfo

	// mapper is a reference to the source.Transformer used to map Parquet fields to a defined schema of the target table.
	mapper Transformer

	// isOpen indicates whether the ParquetReader is currently open and ready for processing.
	isOpen bool

	// wasClosed indicates whether the ParquetReader was closed after being opened.
	wasClosed bool

	// lastError stores the most recent error encountered by the ParquetReader, or nil if no errors occurred.
	lastError error

	// file represents the underlying os.File, used to read the current Parquet file's data.
	file *os.File

	// parquetFile is a reference to the open Parquet file being processed by the ParquetReader.
	parquetFile *parquet.File

	// rowCount represents the total number of rows in the Parquet file being processed.
	rowCount int64

	// channel is a channel used for asynchronously receiving parsed rows from the Parquet file during processing.
	channel chan NextRow

	// nextRow the data of the current row, represented as a slice of interface{} to accommodate any type.
	nextRow []any

	// rowCounter keeps track of the number of rows processed by the ParquetReader during iteration.
	rowCounter int64
}

// NextRow represents a single row of data and an associated error, returned from the channel as a single structure.
type NextRow struct {
	// row represents a single row of data, stored as a slice of interface{} to accommodate various data types.
	row []any

	// err represents an error encountered during the processing of the current row, or nil if no error occurred.
	err error
}

// NewParquetReader creates a new instance of ParquetReader using the supplied FileInfo and Transformer.
func NewParquetReader(file FileInfo, transformer Transformer) *ParquetReader {
	reader := ParquetReader{
		fileInfo: file,
		mapper:   transformer,
	}
	return &reader
}

// IsEmpty returns true if the source Parquet file is empty, or if there is an error in the processing
func (r *ParquetReader) IsEmpty() bool {
	r.OpenAndStartReadingIfNotDoneYet()
	return r.rowCount <= 0 || r.lastError != nil
}

// Next attempts to establish or maintain the reader's state, returning true if no error occurs and false otherwise.
// It implements the interface pgx.CopyFromSource
func (r *ParquetReader) Next() bool {
	r.OpenAndStartReadingIfNotDoneYet()
	if r.lastError != nil {
		return false
	}
	data, ok := <-r.channel
	if !ok {
		// r.lastError = io.EOF // this caused a bug with small tables
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
	fileName := fileInfo.LocalPath
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
	log.Debug(fmt.Sprintf(`Row count = %d`, r.rowCount))

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

// StartReading reads rows from a parquet file using a transformer and starts a goroutine to process rows asynchronously.
func (r *ParquetReader) StartReading() (int, error) {
	log.Trace("f.Schema(): ", zap.String("name", r.parquetFile.Schema().Name()))
	for i, column := range r.parquetFile.Schema().Columns() {
		for j, path := range column {
			log.Trace("Column", zap.Int("i", i), zap.Int("j", j), zap.String("localPath", path))
		}
	}

	for i, rowGroup := range r.parquetFile.RowGroups() {
		log.Trace("RowGroup: ", zap.Int("index", i))
		for j, columnChunk := range rowGroup.ColumnChunks() {
			log.Trace("ColumnChunk: ", zap.Int("index", j), zap.Int("column", columnChunk.Column()),
				zap.Any("type", columnChunk.Type()))
		}
	}

	r.channel = make(chan NextRow)

	go func() {
		defer func(r *ParquetReader) {
			err := r.Close()
			if err != nil {
				log.Error("ERROR: ", zap.Error(err))
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
					log.Error("Error reading row", zap.Error(err))
					break
				}

				if rowCount != 1 {
					err = fmt.Errorf("the row count is not 1")
				}

				singleRow := row[0]
				log.Trace("singleRow", zap.Any("singleRow", singleRow))

				var rowData = NextRow{
					row: make([]any, len(singleRow)),
					err: err,
				}
				for i, x := range singleRow {
					rowData.row[i], err = r.mapper.Transform(x)
					if err != nil {
						log.Error("Error transforming row", zap.Int("index", i),
							zap.Any("value", x), zap.Any("row", row), zap.Error(err))
						close(r.channel)
						return
					}
				}

				r.channel <- rowData

				log.Trace("Row", zap.Any("row", row), zap.Int64("rowCounter", r.rowCounter),
					zap.Int("rowCount", rowCount))
				// Process the row as needed
			}
		}

		close(r.channel)
	}()

	return int(r.rowCount), nil
}

func (r *ParquetReader) OpenAndStartReadingIfNotDoneYet() {
	if r.lastError == nil {
		if !r.isOpen && !r.wasClosed {
			r.lastError = r.Open(r.fileInfo)
			if r.lastError == nil {
				count, err := r.StartReading()
				log.Debug("ParquetReader.Next(): r.IsEmpty()", zap.Int("count", count), zap.Error(err))
				if err != nil {
					r.lastError = err
				} else if count == 0 {
					r.lastError = io.EOF
				}
			}
		}
	}
}

// LastError returns the most recent error encountered by the ParquetReader or nil if no errors have occurred.
func (r *ParquetReader) LastError() error {
	return r.lastError
}

// RowCount returns the total number of rows in the Parquet file being processed by the ParquetReader.
func (r *ParquetReader) RowCount() int64 {
	return r.rowCount
}
