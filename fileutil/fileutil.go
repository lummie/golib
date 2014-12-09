package fileutil

// Various utility functions related to the file system

import (
	"os"
)

// A simple wrapper that checks if a file exists and returns a boolean
func FileExists(filename string) bool {
	if _, err := os.Stat(filename); err == nil {
		return true
	}
	return false
}
