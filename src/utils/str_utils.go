package utils

import (
	"path/filepath"
	"strings"
)

// FindFilePathCharacters checks if a string contains illegal file path characters like ".." or the system path separator.
func FindFilePathCharacters(s string) bool {
	return strings.Contains(s, "..") || strings.ContainsRune(s, filepath.Separator)
}
