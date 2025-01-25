package main

import (
	"fmt"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"strings"
)

// LocalSource implementation of a local data source with an AWS RDS database export
type LocalSource struct {
	// snapshotName the name of the snapshot associated with the source.
	// This snapshot name (or export name) is critical because the folder and file names use it actively.
	snapshotName string
	// localDir an absolute localPath to a local folder ending with the same name as the snapshotName
	localDir string
}

// NewLocalSource is a constructor for creating a new LocalSource.
//
// - localDir: is the localPath to a local directory on the filesystem that will be used
// by the LocalSource instance. It must point to an existing directory and will
// be normalized to the current OS localPath format. If the localPath does not exist or
// is not a directory, the function will terminate the program with a fatal log.
func NewLocalSource(localDir string) *LocalSource {
	// Normalize the localDir localPath to the current OS format
	localDir = filepath.Clean(localDir)
	if info, err := os.Stat(localDir); err != nil {
		log.Fatal("Failed to access localDir: %v", zap.Error(err))
	} else if !info.IsDir() {
		log.Fatal("localDir is not a directory: %s", zap.String("localDir", localDir))
	}

	// Extract the last subfolder name from localDir
	lastSubfolder := filepath.Base(localDir)
	//log.Printf("The last subfolder in the localPath is: %s", lastSubfolder)
	return &LocalSource{localDir: localDir, snapshotName: lastSubfolder}
}

func (l *LocalSource) getFile(path string) FileInfo {
	// Concatenate localDir with the given localPath using correct file localPath delimiters
	fullPath := filepath.Join(l.localDir, path)
	// Check if the file exists
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		log.Error("File does not exist: %s", zap.String("fullPath", fullPath))
		return FileInfo{} // Return an empty File if file doesn't exist
	}

	info, err := os.Stat(fullPath)
	if err != nil {
		log.Error("Error retrieving file %s info: %v", zap.String("fullPath", fullPath), zap.Error(err))
		return FileInfo{}
	}

	fileSize := info.Size()
	return FileInfo{relativePath: path, localPath: fullPath, size: fileSize, temp: false}
}

func (l *LocalSource) Dispose(file FileInfo) {
	if file.temp {
		err := os.Remove(file.localPath) // Delete the file
		if err != nil {
			log.Error("Failed to delete file: %v", zap.Error(err))
		}
	}
}

func (l *LocalSource) getSnapshotName() string {
	return l.snapshotName
}

func (l *LocalSource) listFiles(relativePath string, fileMask string, foldersOnly bool) ([]string, error) {
	var files []string

	dir := l.getFile(relativePath)
	if dir.localPath == "" {
		return []string{}, fmt.Errorf("localPath not found: %s", relativePath)
	}

	entries, err := os.ReadDir(dir.localPath)
	if err != nil {
		return []string{}, fmt.Errorf("error accessing directory %s: %w", dir.localPath, err)
	}

	prefix, suffix := splitMask(fileMask)

	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) && strings.HasSuffix(entry.Name(), suffix) {
			if !foldersOnly || entry.IsDir() {
				entryPath := filepath.Join(dir.relativePath, entry.Name())
				files = append(files, entryPath)
			}
		}
	}

	return files, nil
}

// splitMask Split the fileMask into prefix and suffix by the "*" delimiter
func splitMask(fileMask string) (prefix string, suffix string) {
	splitMask := strings.SplitN(fileMask, "*", 2)
	if len(splitMask) > 1 {
		// If there's a "*", assign the parts accordingly
		prefix, suffix = splitMask[0], splitMask[1]
	} else {
		// If there's no "*", assign the entire fileMask to prefix and suffix to empty
		prefix = fileMask
		suffix = ""
	}
	return
}

func (l *LocalSource) listFilesRecursively(relativePath string) (ret []string, err error) {
	dir := l.getFile(relativePath)
	if dir.localPath == "" {
		return []string{}, fmt.Errorf("localPath not found: %s", relativePath)
	}

	entries, err := os.ReadDir(dir.localPath)
	if err != nil {
		return []string{}, fmt.Errorf("error accessing directory %s: %w", dir.localPath, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			entryPath := filepath.Join(dir.relativePath, entry.Name())
			subFiles, err := l.listFilesRecursively(entryPath)
			if err != nil {
				return []string{}, err
			}
			ret = append(ret, subFiles...)
		} else {
			ret = append(ret, filepath.Join(dir.relativePath, entry.Name()))
		}
	}

	return ret, nil
}
