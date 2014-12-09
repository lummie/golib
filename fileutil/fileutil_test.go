package fileutil

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestFileThatDoesNotExist(t *testing.T) {
	filename := "/tmp/FileThatShouldNotExist"
	exists := FileExists(filename)
	if exists {
		t.Error("Filename should not exist", filename)
	}
}

func TestFileThatDoesExist(t *testing.T) {
	filename := "/tmp/FileThatShouldNotExist"

	// create the file
	err := ioutil.WriteFile(filename, []byte("hello"), 0644)
	if err != nil {
		t.Error("Failed to create file", filename)
	}

	// check it exists
	exists := FileExists(filename)
	if !exists {
		t.Error("Filename should exist", filename)
	}

	// clean up file after test
	err = os.Remove(filename)
	if err != nil {
		t.Error("Failed to remove the file", filename)
	}
}
