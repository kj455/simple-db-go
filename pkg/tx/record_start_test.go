package tx

import (
	"testing"

	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/log"
	"github.com/kj455/simple-db/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestNewStartRecord(t *testing.T) {
	t.Parallel()
	const txNum = 1
	page := file.NewPage(8)
	page.SetInt(OffsetOp, uint32(OP_START))
	page.SetInt(OffsetTxNum, uint32(txNum))

	record := NewStartRecord(page)

	assert.Equal(t, OP_START, record.Op())
	assert.Equal(t, txNum, record.TxNum())
	assert.NoError(t, record.Undo(nil))
	assert.Equal(t, "<START 1>", record.String())
}

func TestWriteStartRecordToLog(t *testing.T) {
	t.Parallel()
	const (
		txNum     = 1
		blockSize = 400
		fileName  = "file"
	)
	dir, cleanup := testutil.SetupDir("test_write_start_record_to_log")
	t.Cleanup(cleanup)
	fileMgr := file.NewFileMgr(dir, blockSize)
	lm, err := log.NewLogMgr(fileMgr, fileName)
	assert.NoError(t, err)

	lsn, err := WriteStartRecordToLog(lm, txNum)

	assert.NoError(t, err)
	assert.Equal(t, 1, lsn)

	iter, err := lm.Iterator()

	assert.NoError(t, err)
	assert.True(t, iter.HasNext())

	record, err := iter.Next()

	assert.NoError(t, err)

	page := file.NewPageFromBytes(record)

	assert.Equal(t, OP_START, Op(page.GetInt(OffsetOp)))
	assert.Equal(t, txNum, int(page.GetInt(OffsetTxNum)))
}
