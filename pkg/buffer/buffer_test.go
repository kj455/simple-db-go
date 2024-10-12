package buffer

import (
	"testing"

	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestBuffer_WriteContents(t *testing.T) {
	const (
		blockSize   = 400
		logFileName = "test_buffer_write_contents"
		txNum       = 1
		lsn         = 2
	)
	dir, _, cleanup := testutil.SetupFile(logFileName)
	defer cleanup()
	fileMgr := file.NewFileMgr(dir, blockSize)
	logMgr, err := log.NewLogMgr(fileMgr, logFileName)
	assert.NoError(t, err)
	buf := NewBuffer(fileMgr, logMgr, blockSize)

	buf.WriteContents(txNum, lsn, func(p ReadWritePage) {
		p.SetInt(100, 200)
	})

	assert.Equal(t, uint32(200), buf.contents.GetInt(100))
	assert.Equal(t, txNum, buf.ModifyingTx())
	assert.Equal(t, lsn, buf.lsn)
}

func TestBuffer_Flush(t *testing.T) {
	t.Parallel()
	const (
		blockSize = 400
		tx        = 1
		lsn       = 2
	)
	t.Run("skip flush", func(t *testing.T) {
		t.Parallel()
		const logFileName = "test_buffer_flush_skip"
		dir, _, cleanup := testutil.SetupFile(logFileName)
		defer cleanup()
		fileMgr := file.NewFileMgr(dir, blockSize)
		logMgr, err := log.NewLogMgr(fileMgr, logFileName)
		assert.NoError(t, err)
		buf := NewBuffer(fileMgr, logMgr, blockSize)

		buf.Flush()

		assert.Equal(t, INIT_TX_NUM, buf.ModifyingTx())
		assert.Equal(t, INIT_LSN, buf.lsn)
	})
	t.Run("flush", func(t *testing.T) {
		t.Parallel()
		const logFileName = "test_buffer_flush"
		dir, _, cleanup := testutil.SetupFile(logFileName)
		defer cleanup()
		fileMgr := file.NewFileMgr(dir, blockSize)
		logMgr, err := log.NewLogMgr(fileMgr, logFileName)
		assert.NoError(t, err)
		buf := NewBuffer(fileMgr, logMgr, blockSize)
		buf.block = file.NewBlockId(logFileName, 0)
		// setup not flushed buffer
		buf.logMgr.Append([]byte("test"))
		buf.WriteContents(tx, lsn, func(p ReadWritePage) {
			p.SetInt(100, 200)
		})

		buf.Flush()

		assert.Equal(t, INIT_TX_NUM, buf.ModifyingTx())
		assert.Equal(t, lsn, buf.lsn)
		iter, err := logMgr.Iterator()
		assert.NoError(t, err)
		assert.True(t, iter.HasNext())
		record, err := iter.Next()
		assert.NoError(t, err)
		assert.Equal(t, []byte("test"), record)
	})
}

func TestBuffer_AssignToBlock__(t *testing.T) {
	const (
		blockSize   = 400
		blockNum    = 0
		tx          = 1
		lsn         = 2
		logFileName = "test_buffer_assign_to_block"
	)
	dir, _, cleanup := testutil.SetupFile(logFileName)
	defer cleanup()
	fileMgr := file.NewFileMgr(dir, blockSize)
	logMgr, err := log.NewLogMgr(fileMgr, logFileName)
	assert.NoError(t, err)
	buf := NewBuffer(fileMgr, logMgr, blockSize)
	buf.pins = 99
	// setup block content
	page := file.NewPage(blockSize)
	page.SetInt(100, 200)
	block := file.NewBlockId(logFileName, blockNum)
	fileMgr.Write(block, page)

	buf.AssignToBlock(block)

	assert.Equal(t, block, buf.Block())
	assert.Equal(t, 0, buf.pins)
	assert.Equal(t, uint32(200), buf.Contents().GetInt(100))
}
