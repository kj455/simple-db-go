package log

import (
	"testing"

	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestNewLogIterator(t *testing.T) {
	t.Parallel()
	const (
		fileName  = "file"
		blockSize = 8
	)
	dir, cleanup := testutil.SetupDir("test_log_iterator")
	t.Cleanup(cleanup)
	fileMgr := file.NewFileMgr(dir, blockSize)
	writePage := file.NewPage(blockSize)
	writePage.SetInt(0, blockSize)
	fileMgr.Write(file.NewBlockId(fileName, 0), writePage)

	block := file.NewBlockId(fileName, 0)
	li, err := NewLogIterator(fileMgr, block)

	assert.NoError(t, err)
	assert.Equal(t, blockSize, li.curOffset)
}

func TestLogIterator_HasNext(t *testing.T) {

	t.Run("offset is less than block size", func(t *testing.T) {
		const (
			blockSize = 4096
			filename  = "file"
		)
		dir, cleanup := testutil.SetupDir("test_log_iterator_has_next")
		t.Cleanup(cleanup)
		fileMgr := file.NewFileMgr(dir, blockSize)
		block := file.NewBlockId(filename, 0)

		li, err := NewLogIterator(fileMgr, block)
		li.curOffset = blockSize - 1

		assert.NoError(t, err)
		assert.True(t, li.HasNext())

		li.curOffset = blockSize
		assert.False(t, li.HasNext())
	})
	t.Run("block number is greater than 0", func(t *testing.T) {
		const (
			blockSize = 4096
			filename  = "test_log_iterator_has_next"
		)
		dir, cleanup := testutil.SetupDir(filename)
		t.Cleanup(cleanup)
		fileMgr := file.NewFileMgr(dir, blockSize)
		block := file.NewBlockId(filename, 1)

		li, err := NewLogIterator(fileMgr, block)

		assert.NoError(t, err)
		assert.True(t, li.HasNext())
	})
}

func TestLogIterator_Next(t *testing.T) {
	t.Parallel()
	t.Run("not finished", func(t *testing.T) {
		t.Parallel()
		const (
			blockSize = 14
			record    = "record"
			filename  = "file"
		)
		dir, cleanup := testutil.SetupDir("test_log_iterator_next_not_finished")
		t.Cleanup(cleanup)
		fileMgr := file.NewFileMgr(dir, blockSize)
		block := file.NewBlockId(filename, 0)
		page := file.NewPage(blockSize)
		// setup record in the page
		page.SetInt(0, 4)
		page.SetBytes(4, []byte(record))
		fileMgr.Write(block, page)

		li, err := NewLogIterator(fileMgr, block)
		assert.NoError(t, err)

		got, err := li.Next()

		assert.NoError(t, err)
		assert.Equal(t, []byte(record), got)
		assert.Equal(t, blockSize, li.curOffset)
	})
	t.Run("block finished", func(t *testing.T) {
		t.Parallel()
		const (
			blockSize = 12
			record    = "record"
			filename  = "file"
		)
		dir, cleanup := testutil.SetupDir("test_log_iterator_next_block_finished")
		t.Cleanup(cleanup)
		fileMgr := file.NewFileMgr(dir, blockSize)
		block0 := file.NewBlockId(filename, 0)
		page0 := file.NewPage(blockSize)
		page0.SetInt(0, 4) // second record
		fileMgr.Write(block0, page0)

		block1 := file.NewBlockId(filename, 1)
		page1 := file.NewPage(blockSize)
		page1.SetInt(0, blockSize) // finished
		fileMgr.Write(block1, page1)

		li, err := NewLogIterator(fileMgr, block1)
		assert.NoError(t, err)
		li.curOffset = blockSize

		_, err = li.Next()

		assert.NoError(t, err)
		assert.Equal(t, block0, li.block)
		assert.Equal(t, 4+4, li.curOffset) // first record
	})
}
