package transaction

import (
	"testing"

	"github.com/kj455/simple-db/pkg/buffer"
	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/log"
	"github.com/kj455/simple-db/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestRecoveryMgr_Rollback(t *testing.T) {
	t.Parallel()
	const (
		txNum        = 1
		blockSize    = 4096
		testFileName = "test_recovery_mgr_rollback"
	)
	_, logMgr, buf, _, tx, cleanup := setupRecoveryMgrTest(t, testFileName)
	t.Cleanup(cleanup)
	recoveryMgr := tx.recoveryMgr

	recoveryMgr.SetInt(buf, 100, 1)
	recoveryMgr.SetString(buf, 200, "test")
	recoveryMgr.Commit()
	recoveryMgr.SetInt(buf, 100, 2)
	recoveryMgr.Rollback()

	iter, err := logMgr.Iterator()
	assert.NoError(t, err)
	recs := newLogRecordsFromIter(iter)

	assert.Equal(t, OP_ROLLBACK, recs[0].Op())
	assert.Equal(t, OP_SET_INT, recs[1].Op())
	assert.Equal(t, OP_COMMIT, recs[2].Op())
	assert.Equal(t, OP_SET_STRING, recs[3].Op())
	assert.Equal(t, OP_SET_INT, recs[4].Op())
	assert.Equal(t, OP_START, recs[5].Op())

	assert.Equal(t, uint32(1), buf.Contents().GetInt(100))
	assert.Equal(t, "test", buf.Contents().GetString(200))
}

func TestRecoveryMgr_Recover(t *testing.T) {
	t.Parallel()
	const (
		txNum        = 1
		blockSize    = 4096
		testFileName = "test_recovery_mgr_recover"
	)
	_, logMgr, buf, _, tx, cleanup := setupRecoveryMgrTest(t, testFileName)
	t.Cleanup(cleanup)
	recoveryMgr := tx.recoveryMgr

	recoveryMgr.SetInt(buf, 100, 1)
	recoveryMgr.SetString(buf, 200, "test")
	recoveryMgr.Commit()
	_, err := WriteCheckpointRecordToLog(logMgr)
	assert.NoError(t, err)
	recoveryMgr.SetInt(buf, 100, 2)
	recoveryMgr.Recover()

	iter, err := logMgr.Iterator()
	assert.NoError(t, err)
	recs := newLogRecordsFromIter(iter)

	assert.Equal(t, OP_COMMIT, recs[0].Op())
	assert.Equal(t, OP_SET_INT, recs[1].Op())
	assert.Equal(t, OP_CHECKPOINT, recs[2].Op())
	assert.Equal(t, OP_COMMIT, recs[3].Op())
	assert.Equal(t, OP_SET_STRING, recs[4].Op())
	assert.Equal(t, OP_SET_INT, recs[5].Op())

	assert.Equal(t, uint32(2), buf.Contents().GetInt(100))
	assert.Equal(t, "", buf.Contents().GetString(200))
}

func setupRecoveryMgrTest(t *testing.T, testFileName string) (file.FileMgr, log.LogMgr, buffer.Buffer, buffer.BufferMgr, *TransactionImpl, func()) {
	const blockSize = 4096
	dir, cleanup := testutil.SetupDir("test_recovery_mgr")
	fileMgr := file.NewFileMgr(dir, blockSize)
	logMgr, err := log.NewLogMgr(fileMgr, testFileName)
	assert.NoError(t, err)
	buf := buffer.NewBuffer(fileMgr, logMgr, blockSize)
	bufferMgr := buffer.NewBufferMgr([]buffer.Buffer{buf})
	bufferMgr.Pin(file.NewBlockId(testFileName, 0))
	txNumGen := NewTxNumberGenerator()
	tx, err := NewTransaction(fileMgr, logMgr, bufferMgr, txNumGen)
	assert.NoError(t, err)
	return fileMgr, logMgr, buf, bufferMgr, tx, cleanup
}

func newLogRecordsFromIter(iter log.LogIterator) []LogRecord {
	var recs []LogRecord
	for iter.HasNext() {
		bytes, err := iter.Next()
		if err != nil {
			break
		}
		rec, err := NewLogRecord(bytes)
		if err != nil {
			break
		}
		recs = append(recs, rec)
	}
	return recs
}
