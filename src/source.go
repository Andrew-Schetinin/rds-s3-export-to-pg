package main

// File represents a file to be processed - may be temporary
type File struct {
	// relativePath specifies the file path relative to Source. Used for addressing files in the remote data source.
	relativePath string
	// localPath an absolute path of a local file (downloaded from a remote data source if needed)
	localPath string
	// size the file size in bytes - important for Parquet APIs
	size int64
	// temp indicates that the file is temporary and must be removed by this program at the end (downloaded from S3)
	temp bool
}

type Source interface {

	// getSnapshotName returns the name of the snapshot associated with the source.
	// This snapshot name (or export name) is critical because the folder and file names use it actively.
	getSnapshotName() string

	// getFile returns a file structure, matching the provided relative localPath.
	// The returned file structure points to a local file (with an aboslute localPath),
	// where the file may be downloaded from a remote storage and kept temporarily
	// for duration of the program execution only.
	getFile(relativePath string) File

	// listFiles returns a list of relative file paths as strings within the directory specified
	// by the given relative localPath and matching the given mask (for example "*.json").
	// Only simple masks with a single "*" are supported right now.
	// The returned file names can be used in the getFile function.
	// It returns an error if the directory cannot be accessed or processed.
	listFiles(relativePath string, fileMask string) ([]string, error)

	// Dispose this method must be called for every returned file when it is not needed anymore.
	// It will make sure all temporary files are removed and not use disk space when not needed.
	// If the file is not a temporary file, this method does nothing.
	Dispose(file File)
}
