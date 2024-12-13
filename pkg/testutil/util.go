package testutil

import (
	"os"
	"path/filepath"
)

const testDir = ".tmp"

// SetupFile creates a file in the test directory and returns the directory, file, and cleanup function.
func SetupFile(filename string) (dir string, f *os.File, cleanup func()) {
	path := filepath.Join(RootDir(), testDir, filename)
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	cleanup = func() {
		_ = os.Remove(path)
	}
	return filepath.Join(RootDir(), testDir), f, cleanup
}

func RootDir() string {
	currentDir, err := os.Getwd()
	if err != nil {
		return ""
	}

	for {
		_, err := os.ReadFile(filepath.Join(currentDir, "go.mod"))
		if os.IsNotExist(err) {
			if currentDir == filepath.Dir(currentDir) {
				return ""
			}
			currentDir = filepath.Dir(currentDir)
			continue
		} else if err != nil {
			return ""
		}
		break
	}
	return currentDir
}

func CleanupDir(dir string) {
	_ = os.RemoveAll(dir)
	_ = os.MkdirAll(dir, os.ModePerm)
}
