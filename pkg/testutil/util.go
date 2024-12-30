package testutil

import (
	"os"
	"path/filepath"
)

const testDir = ".tmp"

func SetupDir(dirname string) (dir string, cleanup func()) {
	path := filepath.Join(rootDir(), testDir, dirname)
	_ = os.MkdirAll(path, os.ModePerm)
	cleanup = func() {
		_ = os.RemoveAll(path)
	}
	return path, cleanup
}

func rootDir() string {
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
