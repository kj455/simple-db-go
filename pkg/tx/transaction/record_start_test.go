package transaction

import (
	"testing"

	fmock "github.com/kj455/db/pkg/file/mock"
	lmock "github.com/kj455/db/pkg/log/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestNewStartRecord(t *testing.T) {
	const txNum = 1
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	page := fmock.NewMockPage(ctrl)
	page.EXPECT().GetInt(OpSize).Return(uint32(txNum))

	record := NewStartRecord(page)

	assert.Equal(t, START, record.Op())
	assert.Equal(t, txNum, record.TxNum())
}

func TestStartRecordOp(t *testing.T) {
	record := StartRecord{}
	assert.Equal(t, START, record.Op())
}

func TestStartRecordTxNum(t *testing.T) {
	const txNum = 1
	record := StartRecord{
		txNum: txNum,
	}
	assert.Equal(t, txNum, record.TxNum())
}

func TestStartRecordUndo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	const txNum = 1
	record := StartRecord{
		txNum: txNum,
	}
	record.Undo(nil)
}

func TestStartRecordString(t *testing.T) {
	const txNum = 1
	record := StartRecord{
		txNum: txNum,
	}
	assert.Equal(t, "<START 1>", record.String())
}

func TestWriteStartRecordToLog(t *testing.T) {
	const (
		txNum = 1
		lsn   = 2
	)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	lm := lmock.NewMockLogMgr(ctrl)
	lm.EXPECT().Append([]byte{
		0, 0, 0, 1, // START
		0, 0, 0, 1, // txNum
	}).Return(lsn, nil)

	got, err := WriteStartRecordToLog(lm, txNum)

	assert.NoError(t, err)
	assert.Equal(t, lsn, got)
}
