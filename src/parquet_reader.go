package main

import (
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
	"go.uber.org/zap"
)

func readParquet(localFilePath string) {

	// Open the Parquet file
	fileName := localFilePath // "/Users/andrews/Downloads/part-00000-d68fd39b-0c1b-401b-aeb2-ee1f8ded89dc-c000.gz.parquet"
	//"/Users/andrews/Downloads/flights-1m.parquet" // "/Users/andrews/Downloads/mtcars.parquet"
	file, err := os.Open(fileName)
	if err != nil {
		logger.Error("Failed to open file: "+fileName, zap.Error(err))
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			logger.Error("ERROR: ", zap.Error(err))
		}
	}(file)

	var fileInfo, err2 = file.Stat()
	if err2 != nil {
		logger.Error("Failed to get file info: "+fileName, zap.Error(err2))
		return
	}
	size := fileInfo.Size()
	f, err := parquet.OpenFile(file, size)
	if err != nil {
		logger.Error("Failed to open file: "+fileName, zap.Error(err))
	}
	rowCount := f.NumRows()
	logger.Debug(fmt.Sprintf(`Row count = %d`, rowCount))

	logger.Debug("f.Schema(): ", zap.String("name", f.Schema().Name()))
	for i, column := range f.Schema().Columns() {
		for j, path := range column {
			logger.Debug("Column", zap.Int("i", i), zap.Int("j", j), zap.String("localPath", path))
		}
	}

	for i, rowGroup := range f.RowGroups() {
		logger.Debug("RowGroup: ", zap.Int("index", i))
		for j, columnChunk := range rowGroup.ColumnChunks() {
			logger.Debug("ColumnChunk: ", zap.Int("index", j))
			columnChunk.Column()
		}
	}

	//f, err := parquet.ReadFile(file, size)
	//if err != nil {
	//	...
	//}

	//// Create a parquet.File from the os.File
	//pFile, err := parquet.NewP.NewNewParquetFile(file)
	//if err != nil {
	//	// Handle the error
	//}
	//defer pFile.Close()
	//
	//// Create a new Parquet reader
	//pqReader, err := reader.NewParquetReader(file, new(User), 4) // 4 goroutines
	//if err != nil {
	//	log.Fatalf("Failed to create Parquet reader: %v", err)
	//}
	//defer pqReader.ReadStop() // Ensure the reader stops after execution
	//
	//// Read data in batches of 1000 records
	//batchSize := 1000
	//totalRows := int(pqReader.GetNumRows())
	//
	//fmt.Printf("Found %d rows in the Parquet file\n", totalRows)
	//
	//batch := make([]User, batchSize) // Allocate a slice for the batch
	//for i := 0; i < totalRows; i += batchSize {
	//	// Calculate the size of the slice to read (batchSize or remaining rows)
	//	rowsToRead := batchSize
	//	if i+batchSize > totalRows {
	//		rowsToRead = totalRows - i
	//		batch = make([]User, rowsToRead) // Resize for the final batch
	//	}
	//
	//	// Read the batch into the pre-allocated slice
	//	if err := pqReader.Read(&batch); err != nil {
	//		log.Fatalf("Failed to read batch at index %d: %v", i, err)
	//	}
	//
	//	// Process the batch
	//	fmt.Printf("Processing batch starting at row %d\n", i)
	//	for _, user := range batch {
	//		fmt.Printf("User: Name=%s, Age=%d\n", user.Name, user.Age)
	//	}
	//}

}
