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

func TestNewSetStringRecord(t *testing.T) {
	const (
		txNum    = 1
		filename = "filename"
		blockNum = 2
		offset   = 3
		val      = "value"
	)
	ctrl := gomock.NewController(t)
	page := fmock.NewMockPage(ctrl)
	page.EXPECT().GetInt(OpSize).Return(uint32(txNum))
	page.EXPECT().GetString(OpSize + 4).Return(filename)
	filenameLen := 4 + 4*8
	page.EXPECT().GetInt(OpSize + 4 + filenameLen).Return(uint32(blockNum))
	page.EXPECT().GetInt(OpSize + 4 + filenameLen + 4).Return(uint32(offset))
	page.EXPECT().GetString(OpSize + 4 + filenameLen + 4 + 4).Return(val)

	record := NewSetStringRecord(page)

	assert.Equal(t, SET_STRING, record.Op())
	assert.Equal(t, txNum, record.TxNum())
	assert.Equal(t, filename, record.block.Filename())
	assert.Equal(t, blockNum, record.block.Number())
	assert.Equal(t, offset, record.offset)
	assert.Equal(t, val, record.val)
}

func TestSetStringRecordOp(t *testing.T) {
	record := SetStringRecord{}
	assert.Equal(t, SET_STRING, record.Op())
}

func TestSetStringRecordTxNum(t *testing.T) {
	const txNum = 1
	record := SetStringRecord{
		txNum: txNum,
	}
	assert.Equal(t, txNum, record.TxNum())
}

func TestSetStringRecordUndo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tx := tmock.NewMockTransaction(ctrl)
	const (
		txNum    = 1
		filename = "filename"
		blockNum = 2
		offset   = 3
		val      = "value"
	)
	record := SetStringRecord{
		txNum:  txNum,
		offset: offset,
		val:    val,
		block:  fmock.NewMockBlockId(ctrl),
	}
	tx.EXPECT().Pin(record.block)
	tx.EXPECT().SetString(record.block, offset, val, false)
	tx.EXPECT().Unpin(record.block)

	record.Undo(tx)
}

func TestSetStringRecordToString(t *testing.T) {
	const (
		txNum    = 1
		filename = "filename"
		blockNum = 2
		offset   = 3
		val      = "value"
	)
	record := SetStringRecord{
		txNum:  txNum,
		offset: offset,
		val:    val,
		block:  file.NewBlockId(filename, blockNum),
	}
	assert.Equal(t, "<SET_STRING 1 [file filename, block 2] 3 value>", record.String())
}

func TestWriteSetStringRecordToLog(t *testing.T) {
	const (
		txNum    = 1
		filename = "filename"
		blockNum = 2
		offset   = 3
		val      = "value"
		lsn      = 1
	)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	lm := lmock.NewMockLogMgr(ctrl)
	block := file.NewBlockId(filename, blockNum)
	lm.EXPECT().Append([]byte{
		0, 0, 0, 5, // SET_STRING
		0, 0, 0, 1, // txNum
		0, 0, 0, 8, // filename length
		'f', 'i', 'l', 'e', 'n', 'a', 'm', 'e', // filename
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // padding
		0, 0, 0, 2, // blockNum
		0, 0, 0, 3, // offset
		0, 0, 0, 5, // val length
		'v', 'a', 'l', 'u', 'e', // val
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // padding
	},
	).Return(1, nil)

	got, err := WriteSetStringRecordToLog(lm, txNum, block, offset, val)
	assert.Equal(t, lsn, got)
	assert.NoError(t, err)
}
