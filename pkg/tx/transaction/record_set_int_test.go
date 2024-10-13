package transaction

import (
	"testing"

	"github.com/kj455/db/pkg/file"
	fmock "github.com/kj455/db/pkg/file/mock"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/testutil"
	tmock "github.com/kj455/db/pkg/tx/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestNewSetIntRecord(t *testing.T) {
	t.Parallel()
	const (
		txNum    = 1
		filename = "filename"
		blockNum = 2
		offset   = 3
		val      = 123
	)
	page := file.NewPageFromBytes([]byte{
		0, 0, 0, byte(OP_SET_INT),
		0, 0, 0, txNum, // txNum
		0, 0, 0, byte(len(filename)), // filename length
		'f', 'i', 'l', 'e', 'n', 'a', 'm', 'e', // filename
		'0', '0', '0', '0', '0', '0', '0', '0', // padding
		'0', '0', '0', '0', '0', '0', '0', '0', // padding
		'0', '0', '0', '0', '0', '0', '0', '0', // padding
		0, 0, 0, blockNum, // blockNum
		0, 0, 0, offset, // offset
		0, 0, 0, val, // val
	})

	record := NewSetIntRecord(page)

	assert.Equal(t, OP_SET_INT, record.Op())
	assert.Equal(t, txNum, record.TxNum())
	assert.Equal(t, filename, record.block.Filename())
	assert.Equal(t, blockNum, record.block.Number())
	assert.Equal(t, offset, record.offset)
	assert.Equal(t, val, record.val)
}

func TestSetIntRecordUndo(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	const (
		txNum    = 1
		filename = "filename"
		blockNum = 2
		offset   = 3
		val      = 123
	)
	record := SetIntRecord{
		txNum:  txNum,
		offset: offset,
		val:    val,
		block:  fmock.NewMockBlockId(ctrl),
	}
	tx := tmock.NewMockTransaction(ctrl)
	tx.EXPECT().Pin(record.block)
	tx.EXPECT().SetInt(record.block, record.offset, record.val, false)
	tx.EXPECT().Unpin(record.block)

	record.Undo(tx)
}

func TestWriteSetIntRecordToLog(t *testing.T) {
	t.Parallel()
	const (
		txNum        = 1
		filename     = "filename"
		blockNum     = 2
		offset       = 3
		val          = 123
		testFileName = "test_write_set_int_record_to_log"
		blockSize    = 400
	)
	dir, _, cleanup := testutil.SetupFile(testFileName)
	defer cleanup()
	fileMgr := file.NewFileMgr(dir, blockSize)
	lm, err := log.NewLogMgr(fileMgr, testFileName)
	assert.NoError(t, err)
	block := file.NewBlockId(filename, blockNum)

	lsn, err := WriteSetIntRecordToLog(lm, txNum, block, offset, val)

	assert.NoError(t, err)
	assert.Equal(t, 1, lsn)

	iter, err := lm.Iterator()
	assert.NoError(t, err)
	assert.True(t, iter.HasNext())

	record, err := iter.Next()
	assert.NoError(t, err)

	page := file.NewPageFromBytes(record)
	setIntRecord := NewSetIntRecord(page)

	assert.Equal(t, OP_SET_INT, setIntRecord.Op())
	assert.Equal(t, txNum, setIntRecord.TxNum())
	assert.Equal(t, filename, setIntRecord.block.Filename())
	assert.Equal(t, blockNum, setIntRecord.block.Number())
	assert.Equal(t, offset, setIntRecord.offset)
	assert.Equal(t, val, setIntRecord.val)
}
