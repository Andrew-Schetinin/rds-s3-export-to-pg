package source

// FileInfo represents a file to be processed - may be temporary
type FileInfo struct {
	// RelativePath specifies the file path relative to Source. Used for addressing files in the remote data source.
	RelativePath string
	// LocalPath an absolute path of a local file (downloaded from a remote data source if needed)
	LocalPath string
	// Size the file Size in bytes - important for Parquet APIs
	Size int64
	// Temp indicates that the file is temporary and must be removed by this program at the end (downloaded from S3)
	Temp bool
}

type Source interface {

	// getSnapshotName returns the name of the snapshot associated with the source.
	// This snapshot name (or export name) is critical because the folder and file names use it actively.
	getSnapshotName() string

	// GetFile returns a file structure, matching the provided relative LocalPath.
	// The returned file structure points to a local file (with an absolute LocalPath),
	// where the file may be downloaded from a remote storage and kept temporarily
	// for duration of the program execution only.
	GetFile(relativePath string) FileInfo

	// Dispose this method must be called for every returned file when it is not needed anymore.
	// It will make sure all temporary files are removed and not use disk space when not needed.
	// If the file is not a temporary file, this method does nothing.
	Dispose(file FileInfo)

	// listFiles returns a list of relative file paths as strings within the directory specified
	// by the given relative RelativePath and matching the given fileMask (for example "*.json").
	// Only simple masks with a single "*" are supported right now.
	// The returned file names can be used in the getFile function.
	// It returns an error if the directory cannot be accessed or processed.
	listFiles(relativePath string, fileMask string, foldersOnly bool) ([]string, error)

	// ListFilesRecursively returns a list of all file paths within a directory and its subdirectories.
	// It takes a string parameter 'RelativePath' representing the root directory and returns a slice of strings
	// containing the file paths or an error if traversal fails.
	ListFilesRecursively(relativePath string) ([]string, error)
}
