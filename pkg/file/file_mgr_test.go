package file

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kj455/simple-db/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestNewFileMgr(t *testing.T) {
	t.Parallel()
	const blockSize = 4096
	t.Run("new", func(t *testing.T) {
		t.Parallel()
		dbDir := "test_new_file_mgr_new"
		mgr := NewFileMgr(dbDir, blockSize)
		t.Cleanup(func() {
			os.RemoveAll(dbDir)
		})
		assert.Equal(t, dbDir, mgr.dbDir)
		assert.Equal(t, blockSize, mgr.blockSize)
		assert.True(t, mgr.isNew)
	})
	t.Run("existing", func(t *testing.T) {
		t.Parallel()
		dir, cleanup := testutil.SetupDir("test_new_file_mgr_existing")
		t.Cleanup(cleanup)
		mgr := NewFileMgr(dir, blockSize)
		assert.False(t, mgr.isNew)
	})
}

func TestFileMgr_Read(t *testing.T) {
	t.Parallel()
	const blockSize = 4096
	dbDir, cleanup := testutil.SetupDir("test_new_file_mgr_read")
	t.Cleanup(cleanup)
	mgr := NewFileMgr(dbDir, blockSize)

	setupFile := func(fileName string) (f *os.File, cleanup func()) {
		testFilepath := filepath.Join(dbDir, fileName)
		f, err := os.Create(testFilepath)
		assert.NoError(t, err)
		cleanup = func() {
			os.Remove(testFilepath)
		}
		return f, cleanup
	}

	t.Run("Read", func(t *testing.T) {
		t.Parallel()
		const fileName = "read_test"
		f, cleanup := setupFile(fileName)
		t.Cleanup(cleanup)
		bytes := []byte("hello world!!!!")
		_, err := f.Write(bytes)
		assert.NoError(t, err)
		page := NewPage(len(bytes))
		id := NewBlockId(fileName, 0)

		err = mgr.Read(id, page)

		assert.NoError(t, err)
		assert.Equal(t, bytes, page.Contents().Bytes())
	})
	t.Run("Write", func(t *testing.T) {
		t.Parallel()
		const (
			fileName  = "write_test"
			blockSize = 4096
		)
		_, cleanup := setupFile(fileName)
		t.Cleanup(cleanup)
		page := NewPage(blockSize)
		page.SetString(0, "hello world!!!!")
		id := NewBlockId(fileName, 0)

		err := mgr.Write(id, page)

		assert.NoError(t, err)
		fileContent, err := os.ReadFile(filepath.Join(dbDir, fileName))
		assert.NoError(t, err)
		assert.Equal(t, page.Contents().Bytes(), fileContent)
	})
	t.Run("Append", func(t *testing.T) {
		t.Parallel()
		const fileName = "append_test"
		_, cleanup := setupFile(fileName)
		t.Cleanup(cleanup)

		id, err := mgr.Append(fileName)

		assert.NoError(t, err)
		assert.Equal(t, fileName, id.Filename())
		assert.Equal(t, 0, id.Number())
	})
}
