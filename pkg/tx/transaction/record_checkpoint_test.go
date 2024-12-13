package transaction

import (
	"testing"

	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestNewCheckpointRecord(t *testing.T) {
	t.Parallel()
	record := NewCheckpointRecord()

	assert.Equal(t, OP_CHECKPOINT, record.Op())
	assert.Equal(t, dummyTxNum, record.TxNum())
	assert.NoError(t, record.Undo(nil))
	assert.Equal(t, "<CHECKPOINT>", record.String())
}

func TestWriteCheckpointRecordToLog(t *testing.T) {
	t.Parallel()
	const (
		txNum     = 1
		blockSize = 400
		fileName  = "test_write_checkpoint_record_to_log"
	)
	dir, _, cleanup := testutil.SetupFile(fileName)
	defer cleanup()
	fileMgr := file.NewFileMgr(dir, blockSize)
	lm, err := log.NewLogMgr(fileMgr, fileName)
	assert.NoError(t, err)

	lsn, err := WriteCheckpointRecordToLog(lm)

	assert.NoError(t, err)
	assert.Equal(t, 1, lsn)

	iter, err := lm.Iterator()

	assert.NoError(t, err)
	assert.True(t, iter.HasNext())

	record, err := iter.Next()

	assert.NoError(t, err)

	page := file.NewPageFromBytes(record)

	assert.Equal(t, OP_CHECKPOINT, Op(page.GetInt(OffsetOp)))
}
