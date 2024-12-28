package buffermgr

import (
	"testing"

	"github.com/kj455/db/pkg/buffer"
	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestBufferMgr_Pin(t *testing.T) {
	t.Parallel()
	const blockSize = 4096
	t.Run("success - no buffer assigned with block", func(t *testing.T) {
		t.Parallel()
		const (
			buffNum     = 3
			logFileName = "test_buffer_mgr_pin_no_buffer_assigned"
		)
		dir, _, cleanup := testutil.SetupFile(logFileName)
		t.Cleanup(cleanup)
		fileMgr := file.NewFileMgr(dir, blockSize)
		logMgr, err := log.NewLogMgr(fileMgr, logFileName)
		assert.NoError(t, err)
		buffs := make([]buffer.Buffer, buffNum)
		for i := 0; i < buffNum; i++ {
			buffs[i] = buffer.NewBuffer(fileMgr, logMgr, blockSize)
		}
		bm := NewBufferMgr(buffs, WithMaxWaitTime(0))
		assert.Equal(t, buffNum, bm.AvailableNum())
		blk := file.NewBlockId(logFileName, 0)

		buff, err := bm.Pin(blk)

		assert.NoError(t, err)
		assert.Equal(t, buffNum-1, bm.AvailableNum())
		assert.Equal(t, blk, buff.Block())
	})
	t.Run("success - already pinned", func(t *testing.T) {
		t.Parallel()
		const (
			logFileName = "test_buffer_mgr_pin_already_pinned"
			buffNum     = 1
		)
		dir, _, cleanup := testutil.SetupFile(logFileName)
		t.Cleanup(cleanup)
		fileMgr := file.NewFileMgr(dir, blockSize)
		logMgr, err := log.NewLogMgr(fileMgr, logFileName)
		assert.NoError(t, err)
		buffs := make([]buffer.Buffer, buffNum)
		for i := 0; i < buffNum; i++ {
			buffs[i] = buffer.NewBuffer(fileMgr, logMgr, blockSize)
		}
		bm := NewBufferMgr(buffs, WithMaxWaitTime(0))
		blk := file.NewBlockId(logFileName, 0)
		// setup: pin the buffer
		_, err = bm.Pin(blk)
		assert.NoError(t, err)
		assert.Equal(t, buffNum-1, bm.AvailableNum())

		buff, err := bm.Pin(blk)

		assert.NoError(t, err)
		assert.Equal(t, blk, buff.Block())
		assert.Equal(t, buffNum-1, bm.AvailableNum())
	})
	t.Run("fail - no available buffer", func(t *testing.T) {
		t.Parallel()
		const (
			logFileName = "test_buffer_mgr_pin_no_available_buffer"
			buffNum     = 1
		)
		dir, _, cleanup := testutil.SetupFile(logFileName)
		t.Cleanup(cleanup)
		fileMgr := file.NewFileMgr(dir, blockSize)
		logMgr, err := log.NewLogMgr(fileMgr, logFileName)
		assert.NoError(t, err)
		buffs := make([]buffer.Buffer, buffNum)
		for i := 0; i < buffNum; i++ {
			buffs[i] = buffer.NewBuffer(fileMgr, logMgr, blockSize)
		}
		bm := NewBufferMgr(buffs, WithMaxWaitTime(0))
		blk := file.NewBlockId(logFileName, 0)
		// setup: all buffers are pinned
		_, err = bm.Pin(blk)
		assert.NoError(t, err)

		blk2 := file.NewBlockId(logFileName, 1)
		_, err = bm.Pin(blk2)

		assert.Error(t, err)
	})
}

func TestBufferMgrImpl_Unpin(t *testing.T) {
	t.Parallel()
	t.Run("availableNum increment if buffer was completely unpinned", func(t *testing.T) {
		t.Parallel()
		const (
			blockSize   = 4096
			logFileName = "test_buffer_mgr_unpin_available_increment"
		)
		dir, _, cleanup := testutil.SetupFile(logFileName)
		t.Cleanup(cleanup)
		fileMgr := file.NewFileMgr(dir, blockSize)
		logMgr, err := log.NewLogMgr(fileMgr, logFileName)
		assert.NoError(t, err)
		buff := buffer.NewBuffer(fileMgr, logMgr, blockSize)
		bm := NewBufferMgr([]buffer.Buffer{buff}, WithMaxWaitTime(0))
		blk := file.NewBlockId(logFileName, 0)

		_, err = bm.Pin(blk)
		assert.NoError(t, err)
		assert.Equal(t, 0, bm.AvailableNum())

		_, err = bm.Pin(blk)
		assert.NoError(t, err)
		assert.Equal(t, 0, bm.AvailableNum())

		bm.Unpin(buff)
		assert.Equal(t, 0, bm.AvailableNum())

		bm.Unpin(buff)
		assert.Equal(t, 1, bm.AvailableNum())
	})
}

func TestBufferMgrImpl_FlushAll(t *testing.T) {
	t.Parallel()
	t.Run("flush only matched txNum", func(t *testing.T) {
		t.Parallel()
		const (
			blockSize   = 4096
			logFileName = "test_buffer_mgr_flush_all"
			txNum       = 1
		)
		dir, _, cleanup := testutil.SetupFile(logFileName)
		t.Cleanup(cleanup)
		fileMgr := file.NewFileMgr(dir, blockSize)
		logMgr, err := log.NewLogMgr(fileMgr, logFileName)
		assert.NoError(t, err)
		buff := buffer.NewBuffer(fileMgr, logMgr, blockSize)
		bm := NewBufferMgr([]buffer.Buffer{buff}, WithMaxWaitTime(0))
		blk := file.NewBlockId(logFileName, 0)
		pBuf, err := bm.Pin(blk)
		assert.NoError(t, err)

		// setup: buffer is modified by txNum 1
		pBuf.WriteContents(txNum, 1, func(p buffer.ReadWritePage) {
			p.SetInt(100, 200)
		})

		// assert: buffer is not flushed
		pageReader := file.NewPage(blockSize)
		fileMgr.Read(blk, pageReader)
		assert.Equal(t, uint32(0), pageReader.GetInt(100))

		// assert: buffer is not flushed if txNum is not matched
		err = bm.FlushAll(txNum + 1)
		assert.NoError(t, err)
		fileMgr.Read(blk, pageReader)
		assert.Equal(t, uint32(0), pageReader.GetInt(100))

		// assert: buffer was flushed
		err = bm.FlushAll(txNum)
		assert.NoError(t, err)
		fileMgr.Read(blk, pageReader)
		assert.Equal(t, uint32(200), pageReader.GetInt(100))
	})
}
