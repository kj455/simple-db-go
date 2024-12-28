package transaction

import (
	"testing"

	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestNewCommitRecord(t *testing.T) {
	t.Parallel()
	const txNum = 1
	page := file.NewPage(8)
	page.SetInt(OffsetOp, uint32(OP_COMMIT))
	page.SetInt(OffsetTxNum, uint32(txNum))

	record := NewCommitRecord(page)

	assert.Equal(t, OP_COMMIT, record.Op())
	assert.Equal(t, txNum, record.TxNum())
	assert.NoError(t, record.Undo(nil))
	assert.Equal(t, "<COMMIT 1>", record.String())
}

func TestWriteCommitRecordToLog(t *testing.T) {
	t.Parallel()
	const (
		txNum     = 1
		blockSize = 400
		fileName  = "test_write_commit_record_to_log"
	)
	dir, _, cleanup := testutil.SetupFile(fileName)
	t.Cleanup(cleanup)
	fileMgr := file.NewFileMgr(dir, blockSize)
	lm, err := log.NewLogMgr(fileMgr, fileName)
	assert.NoError(t, err)

	lsn, err := WriteCommitRecordToLog(lm, txNum)

	assert.NoError(t, err)
	assert.Equal(t, 1, lsn)

	iter, err := lm.Iterator()

	assert.NoError(t, err)
	assert.True(t, iter.HasNext())

	record, err := iter.Next()

	assert.NoError(t, err)

	page := file.NewPageFromBytes(record)

	assert.Equal(t, OP_COMMIT, Op(page.GetInt(OffsetOp)))
	assert.Equal(t, txNum, int(page.GetInt(OffsetTxNum)))
}
