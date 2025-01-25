package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"go.uber.org/zap"
	"io"
	"strings"
)

// NeverHappeningCharacter is a constant representing a rarely-used ASCII character (\x7F)
// for internal placeholder usage. This is the Delete control character.
// - This character is not printable. It is primarily used for legacy systems or specific communication protocols
// (e.g., terminal controls). Therefore, it never supposed to appear in an input string.
// IMPORTANT: We need it for a dirty trick to enforce the "encoding/csv" package to distinguish
// between nil and "" values, which is critical for correct processing in PostgreSQL.
const NeverHappeningCharacter = "\x7F"

func convertToCSVReader(ctx context.Context, source *ParquetReader) (io.Reader, error) {
	pr, pw := io.Pipe() // Create a pipe for streaming

	go func() {
		defer func(pw *io.PipeWriter) {
			err := pw.Close()
			if err != nil {
				logger.Error("Error closing pipe writer", zap.Error(err))
			}
		}(pw) // Close the writer when done

		csvWriter := csv.NewWriter(pw)

		for source.Next() {
			select {
			case <-ctx.Done(): // Check for cancellation
				csvWriter.Flush()
				if err := csvWriter.Error(); err != nil {
					logger.Error("Error during flush after cancellation", zap.Error(err))
				}
				return // Exit goroutine if context is cancelled
			default:
				values, err := source.Values()
				if err != nil {
					logger.Error("Error getting values", zap.Error(err))
					return // Exit goroutine on error
				}

				record := make([]string, len(values))
				for i, v := range values {
					if v == nil {
						record[i] = ""
					} else {
						record[i] = fmt.Sprint(v) // Convert all values to string
						// IMPORTANT: We need it for a dirty trick to enforce the "encoding/csv" package to distinguish
						// between nil and "" values, which is critical for correct processing in PostgreSQL.
						if record[i] == "" {
							record[i] = NeverHappeningCharacter
						}
					}
				}

				if err := csvWriter.Write(record); err != nil {
					logger.Error("Error writing CSV record", zap.Error(err))
					return // Exit goroutine on error
				}
			}
		}

		if err := source.Err(); err != nil {
			logger.Error("Error from source", zap.Error(err))
		}

		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			logger.Error("Error flushing CSV writer", zap.Error(err))
		}
	}()

	newPr := wrapPipeReaderWithProcessing(context.Background(), pr, replaceNeverHappeningCharacter)

	return newPr, nil
}

// replaceNeverHappeningCharacter replaces all occurrences of NeverHappeningCharacter in the input string
// with empty quotes (""). This is because the standard behavior of the "encoding/csv" package is not to wrap
// strings with quotes unless really needed, and for us this character indicates an empty string that
// we want to arrive to PostgreSQL double-quoted.
func replaceNeverHappeningCharacter(s string) string {
	return strings.ReplaceAll(s, NeverHappeningCharacter, "\"\"")
}

func wrapPipeReaderWithProcessing(ctx context.Context, pr *io.PipeReader, processFunc func(string) string) *io.PipeReader {
	r, w := io.Pipe() // Create another pipe for wrapping

	go func() {
		defer func() {
			if err := pr.Close(); err != nil {
				logger.Error("Error closing original pipe reader", zap.Error(err))
			}
			if err := w.Close(); err != nil {
				logger.Error("Error closing new pipe writer", zap.Error(err))
			}
		}()

		buf := make([]byte, 1024) // Buffer for reading from the original reader
		for {
			select {
			case <-ctx.Done():
				logger.Info("Context canceled in wrapPipeReaderWithProcessing")
				return
			default:
				n, err := pr.Read(buf)
				if err != nil {
					if err != io.EOF {
						logger.Error("Error reading from original pipe", zap.Error(err))
					}
					break
				}

				// Preprocess the data using the supplied function
				inputData := string(buf[:n])
				processedData := processFunc(inputData)

				// Write the processed data to the new pipe writer
				_, err = w.Write([]byte(processedData))
				if err != nil {
					logger.Error("Error writing to new pipe", zap.Error(err))
					break
				}
			}
		}
	}()

	return r
}
