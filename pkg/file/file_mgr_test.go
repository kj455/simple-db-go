package file

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFileMgr(t *testing.T) {
	t.Parallel()
	const (
		dbDir     = "test"
		blockSize = 4096
	)
	// new
	mgr := NewFileMgr(dbDir, blockSize)
	assert.NotNil(t, mgr)
	assert.Equal(t, dbDir, mgr.dbDir)
	assert.Equal(t, blockSize, mgr.blockSize)
	assert.False(t, mgr.isNew)

	// existing
	os.Mkdir(dbDir, 0755)
	mgr = NewFileMgr(dbDir, blockSize)
	assert.NotNil(t, mgr)
	assert.True(t, mgr.isNew)
	os.RemoveAll(dbDir)
}

func TestFileMgr(t *testing.T) {
	t.Parallel()
	const (
		blockSize    = 4096
		dbDir        = "test"
		testFilename = "testfile"
	)
	testFilepath := filepath.Join(dbDir, testFilename)
	mgr := NewFileMgr(dbDir, blockSize)
	assert.NotNil(t, mgr)
	setup := func() func() {
		os.Mkdir(dbDir, 0755)
		_, err := os.Create(testFilepath)
		assert.NoError(t, err)
		return func() {
			os.RemoveAll(dbDir)
			os.Remove(testFilepath)
		}
	}
	tests := []struct {
		name string
		fn   func(*testing.T, *FileMgr)
	}{
		{
			name: "Read after Write",
			fn: func(t *testing.T, mgr *FileMgr) {
				id := NewBlockId(testFilename, 0)
				page := NewPage(blockSize)
				page.SetString(0, "hello")
				err := mgr.Write(id, page)
				assert.NoError(t, err)

				readPage := NewPage(blockSize)
				err = mgr.Read(id, readPage)
				assert.NoError(t, err)
				assert.Equal(t, "hello", readPage.GetString(0))
			},
		},
		{
			name: "Read non-existent file",
			fn: func(t *testing.T, mgr *FileMgr) {
				id := NewBlockId("non-existent-file", 0)
				page := NewPage(blockSize)
				err := mgr.Read(id, page)
				assert.Error(t, err)
			},
		},
		{
			name: "Append",
			fn: func(t *testing.T, mgr *FileMgr) {
				id, err := mgr.Append(testFilename)
				assert.NoError(t, err)
				assert.Equal(t, testFilename, id.Filename())
				assert.Equal(t, 0, id.Number())
			},
		},
	}
	for _, tt := range tests {
		cleanup := setup()
		defer cleanup()
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(t, mgr)
		})
	}
}
