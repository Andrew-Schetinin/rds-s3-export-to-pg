package source

import (
	"dbrestore/utils"
	"go.uber.org/zap"
	"os"
)

// log a convenience wrapper to shorten code lines
var log = utils.Logger

type S3Source struct {
	path string
}

func (l S3Source) getFile(path string) FileInfo {
	//TODO implement me
	panic("implement me")
}

func (l S3Source) Dispose(file FileInfo) {
	if file.Temp {
		err := os.Remove(file.LocalPath) // Delete the file
		if err != nil {
			log.Error("Failed to delete file: %v", zap.Error(err))
		}
	}
}
