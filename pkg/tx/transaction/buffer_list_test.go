package transaction

import (
	"testing"

	"github.com/kj455/db/pkg/buffer"
	buffermgr "github.com/kj455/db/pkg/buffer_mgr"
	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestBufferList(t *testing.T) {
	t.Parallel()
	const (
		blockSize    = 4096
		testFileName = "test_buffer_list"
	)
	dir, _, cleanup := testutil.SetupFile(testFileName)
	defer cleanup()
	fileMgr := file.NewFileMgr(dir, blockSize)
	logMgr, err := log.NewLogMgr(fileMgr, testFileName)
	assert.NoError(t, err)
	block1 := file.NewBlockId(testFileName, 0)
	block2 := file.NewBlockId(testFileName, 1)
	buf := buffer.NewBuffer(fileMgr, logMgr, blockSize)
	buf2 := buffer.NewBuffer(fileMgr, logMgr, blockSize)
	bufferMgr := buffermgr.NewBufferMgr([]buffer.Buffer{buf, buf2})
	bufferList := NewBufferList(bufferMgr)

	bufferList.Pin(block1)
	assert.Equal(t, 1, len(bufferList.pins))
	assert.Equal(t, 1, len(bufferList.buffers))
	b, ok := bufferList.GetBuffer(block1)
	assert.True(t, ok)
	assert.Equal(t, buf, b)

	bufferList.Pin(block1)
	assert.Equal(t, 2, len(bufferList.pins))
	assert.Equal(t, 1, len(bufferList.buffers))

	bufferList.Pin(block2)
	assert.Equal(t, 3, len(bufferList.pins))
	assert.Equal(t, 2, len(bufferList.buffers))

	bufferList.Unpin(block1)
	assert.Equal(t, 2, len(bufferList.pins))
	assert.Equal(t, 2, len(bufferList.buffers))

	bufferList.UnpinAll()
	assert.Equal(t, 0, len(bufferList.pins))
	assert.Equal(t, 0, len(bufferList.buffers))
}
