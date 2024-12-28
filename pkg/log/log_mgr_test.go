package log

import (
	"testing"

	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestNewLogMgr(t *testing.T) {
	t.Parallel()
	t.Run("no block", func(t *testing.T) {
		const (
			testFileName = "test_new_log_mgr_first_block"
			blockSize    = 4096
		)
		dir, _, cleanup := testutil.SetupFile(testFileName)
		t.Cleanup(cleanup)
		fileMgr := file.NewFileMgr(dir, blockSize)

		lm, err := NewLogMgr(fileMgr, testFileName)

		assert.NoError(t, err)
		firstBlock := file.NewBlockId(testFileName, 0)
		assert.True(t, lm.currentBlock.Equals(firstBlock))
		assert.Equal(t, blockSize, lm.getLastOffset())
	})

	t.Run("block exists", func(t *testing.T) {
		const (
			testFileName = "test_new_log_mgr_block_exists"
			blockSize    = 5
		)
		dir, f, cleanup := testutil.SetupFile(testFileName)
		t.Cleanup(cleanup)
		record := []byte("hello world!!")
		_, err := f.Write(record)
		f.Close()
		assert.NoError(t, err)
		fileMgr := file.NewFileMgr(dir, blockSize)

		lm, err := NewLogMgr(fileMgr, testFileName)

		assert.NoError(t, err)
		assert.True(t, lm.currentBlock.Equals(file.NewBlockId(testFileName, len("hello world!!")/blockSize)))
		assert.Equal(t, "d!!\x00\x00", string(lm.page.Contents().String()))
	})
}

func TestLogMgr_Append(t *testing.T) {
	t.Parallel()
	t.Run("has space", func(t *testing.T) {
		t.Parallel()
		const (
			testFileName = "test_log_mgr_append_has_space"
			blockSize    = 10
			blockIdx     = 0
		)
		dir, _, cleanup := testutil.SetupFile(testFileName)
		t.Cleanup(cleanup)
		page := file.NewPage(blockSize)
		page.SetInt(0, blockSize)
		fileMgr := file.NewFileMgr(dir, blockSize)
		block := file.NewBlockId(testFileName, blockIdx)
		fileMgr.Write(block, page)
		lm, err := NewLogMgr(fileMgr, testFileName)
		assert.NoError(t, err)
		record := []byte("test")

		lm.Append(record)

		assert.Equal(t, 0, lm.lastSavedLSN)
		assert.Equal(t, 1, lm.latestLSN)
		assert.Equal(t, blockSize-OFFSET_SIZE-len("test"), lm.getLastOffset())
	})
	t.Run("no space", func(t *testing.T) {
		t.Parallel()
		const (
			testFileName = "test_log_mgr_append_no_space"
			blockSize    = 8
			blockIdx     = 0
		)
		dir, _, cleanup := testutil.SetupFile(testFileName)
		t.Cleanup(cleanup)
		page := file.NewPage(blockSize)
		page.SetInt(0, blockSize)
		fileMgr := file.NewFileMgr(dir, blockSize)
		block := file.NewBlockId(testFileName, blockIdx)
		fileMgr.Write(block, page)
		lm, err := NewLogMgr(fileMgr, testFileName)
		assert.NoError(t, err)
		record := []byte("test")

		lm.Append(record)

		assert.Equal(t, 0, lm.lastSavedLSN)
		assert.Equal(t, 1, lm.latestLSN)
		nextBlock := file.NewBlockId(testFileName, blockIdx+1)
		assert.True(t, lm.currentBlock.Equals(nextBlock))
	})
}

func TestLogMgr_Flush(t *testing.T) {
	t.Parallel()
	t.Run("flush past lsn", func(t *testing.T) {
		t.Parallel()
		const (
			testFileName = "test_log_mgr_flush_past_lsn"
			blockSize    = 10
		)
		dir, _, cleanup := testutil.SetupFile(testFileName)
		t.Cleanup(cleanup)
		fileMgr := file.NewFileMgr(dir, blockSize)
		lm, err := NewLogMgr(fileMgr, testFileName)
		assert.NoError(t, err)
		lm.latestLSN = 100
		lm.lastSavedLSN = 99

		lm.Flush(100)

		assert.Equal(t, 100, lm.lastSavedLSN)
		readPage := file.NewPage(blockSize)
		fileMgr.Read(file.NewBlockId(testFileName, 0), readPage)
		assert.Equal(t, int(readPage.GetInt(0)), blockSize)
	})
	t.Run("not flush past lsn", func(t *testing.T) {
		t.Parallel()
		const (
			testFileName = "test_log_mgr_flush_not_past_lsn"
			blockSize    = 10
		)
		dir, _, cleanup := testutil.SetupFile(testFileName)
		t.Cleanup(cleanup)
		fileMgr := file.NewFileMgr(dir, blockSize)
		lm, err := NewLogMgr(fileMgr, testFileName)
		assert.NoError(t, err)
		lm.latestLSN = 100
		lm.lastSavedLSN = 99

		lm.Flush(98)

		assert.Equal(t, 100, lm.latestLSN)
		assert.Equal(t, 99, lm.lastSavedLSN)
	})
}
