package source

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
	// localDir an absolute LocalPath to a local folder ending with the same name as the snapshotName
	localDir string
}

// NewLocalSource is a constructor for creating a new LocalSource.
//
// - localDir: is the LocalPath to a local directory on the filesystem that will be used
// by the LocalSource instance. It must point to an existing directory and will
// be normalized to the current OS LocalPath format. If the LocalPath does not exist or
// is not a directory, the function will terminate the program with a fatal log.
func NewLocalSource(localDir string) *LocalSource {
	// Normalize the localDir LocalPath to the current OS format
	localDir = filepath.Clean(localDir)
	if info, err := os.Stat(localDir); err != nil {
		log.Fatal("Failed to access localDir: %v", zap.Error(err))
	} else if !info.IsDir() {
		log.Fatal("localDir is not a directory: %s", zap.String("localDir", localDir))
	}

	// Extract the last subfolder name from localDir
	lastSubfolder := filepath.Base(localDir)
	//log.Printf("The last subfolder in the LocalPath is: %s", lastSubfolder)
	return &LocalSource{localDir: localDir, snapshotName: lastSubfolder}
}

func (l *LocalSource) GetFile(path string) FileInfo {
	// Concatenate localDir with the given LocalPath using correct file LocalPath delimiters
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
	return FileInfo{RelativePath: path, LocalPath: fullPath, Size: fileSize, Temp: false}
}

func (l *LocalSource) Dispose(file FileInfo) {
	if file.Temp {
		err := os.Remove(file.LocalPath) // Delete the file
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

	dir := l.GetFile(relativePath)
	if dir.LocalPath == "" {
		return []string{}, fmt.Errorf("LocalPath not found: %s", relativePath)
	}

	entries, err := os.ReadDir(dir.LocalPath)
	if err != nil {
		return []string{}, fmt.Errorf("error accessing directory %s: %w", dir.LocalPath, err)
	}

	prefix, suffix := splitMask(fileMask)

	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) && strings.HasSuffix(entry.Name(), suffix) {
			if !foldersOnly || entry.IsDir() {
				entryPath := filepath.Join(dir.RelativePath, entry.Name())
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

func (l *LocalSource) ListFilesRecursively(relativePath string) (ret []string, err error) {
	dir := l.GetFile(relativePath)
	if dir.LocalPath == "" {
		return []string{}, fmt.Errorf("LocalPath not found: %s", relativePath)
	}

	entries, err := os.ReadDir(dir.LocalPath)
	if err != nil {
		return []string{}, fmt.Errorf("error accessing directory %s: %w", dir.LocalPath, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			entryPath := filepath.Join(dir.RelativePath, entry.Name())
			subFiles, err := l.ListFilesRecursively(entryPath)
			if err != nil {
				return []string{}, err
			}
			ret = append(ret, subFiles...)
		} else {
			ret = append(ret, filepath.Join(dir.RelativePath, entry.Name()))
		}
	}

	return ret, nil
}
