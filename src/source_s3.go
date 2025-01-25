package main

import (
	"go.uber.org/zap"
	"os"
)

type S3Source struct {
	path string
}

func (l S3Source) getFile(path string) FileInfo {
	//TODO implement me
	panic("implement me")
}

func (l S3Source) Dispose(file FileInfo) {
	if file.temp {
		err := os.Remove(file.localPath) // Delete the file
		if err != nil {
			log.Error("Failed to delete file: %v", zap.Error(err))
		}
	}
}
