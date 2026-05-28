package source

import (
	"dbrestore/utils"
	"go.uber.org/zap"
	"os"
)

// log a convenience wrapper to shorten code lines
var log = &utils.Logger

type S3Source struct {
}

func (l S3Source) getFile(_ string) FileInfo { //nolint:unused // S3 stub, will be used when S3 source is implemented
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
