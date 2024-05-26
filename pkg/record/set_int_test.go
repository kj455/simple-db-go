package record

import (
	"testing"

	"github.com/kj455/db/pkg/file"
	fmock "github.com/kj455/db/pkg/file/mock"
	lmock "github.com/kj455/db/pkg/log/mock"
	tmock "github.com/kj455/db/pkg/tx/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestNewSetIntRecord(t *testing.T) {
	const (
		txNum    = 1
		filename = "filename"
		blockNum = 2
		offset   = 3
		val      = 123
	)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	page := fmock.NewMockPage(ctrl)
	page.EXPECT().GetInt(OpSize).Return(uint32(txNum))
	page.EXPECT().GetString(OpSize + 4).Return(filename)
	filenameLen := 4 + 4*8
	page.EXPECT().GetInt(OpSize + 4 + filenameLen).Return(uint32(blockNum))
	page.EXPECT().GetInt(OpSize + 4 + filenameLen + 4).Return(uint32(offset))
	page.EXPECT().GetInt(OpSize + 4 + filenameLen + 4 + 4).Return(uint32(val))

	record := NewSetIntRecord(page)

	assert.Equal(t, SET_INT, record.Op())
	assert.Equal(t, txNum, record.TxNum())
	assert.Equal(t, filename, record.block.Filename())
	assert.Equal(t, blockNum, record.block.Number())
	assert.Equal(t, offset, record.offset)
	assert.Equal(t, val, record.val)
}

func TestSetIntRecordOp(t *testing.T) {
	record := SetIntRecord{}
	assert.Equal(t, SET_INT, record.Op())
}

func TestSetIntRecordTxNum(t *testing.T) {
	const txNum = 1
	record := SetIntRecord{
		txNum: txNum,
	}
	assert.Equal(t, txNum, record.TxNum())
}

func TestSetIntRecordUndo(t *testing.T) {
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
	const (
		txNum    = 1
		filename = "filename"
		blockNum = 2
		offset   = 3
		val      = 123
		lsn      = 0
	)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	block := file.NewBlockId(filename, blockNum)
	lm := lmock.NewMockLogMgr(ctrl)
	lm.EXPECT().Append([]byte{
		0, 0, 0, 4, // SET_INT
		0, 0, 0, 1, // txNum
		0, 0, 0, 8, // filename length
		102, 105, 108, 101, 110, 97, 109, 101, // filename
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // padding
		0, 0, 0, 2, // blockNum
		0, 0, 0, 3, // offset
		0, 0, 0, 123, // val
	}).Return(lsn, nil)

	got, err := WriteSetIntRecordToLog(lm, txNum, block, offset, val)
	assert.NoError(t, err)
	assert.Equal(t, lsn, got)
}
