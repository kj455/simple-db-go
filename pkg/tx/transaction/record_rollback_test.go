package transaction

import (
	"testing"

	fmock "github.com/kj455/db/pkg/file/mock"
	lmock "github.com/kj455/db/pkg/log/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestNewRollbackRecord(t *testing.T) {
	const txNum = 1
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	page := fmock.NewMockPage(ctrl)
	page.EXPECT().GetInt(OpSize).Return(uint32(txNum))

	record := NewRollbackRecord(page)

	assert.Equal(t, ROLLBACK, record.Op())
	assert.Equal(t, txNum, record.TxNum())
}

func TestRollbackRecordOp(t *testing.T) {
	record := RollbackRecord{}
	assert.Equal(t, ROLLBACK, record.Op())
}

func TestRollbackRecordTxNum(t *testing.T) {
	const txNum = 1
	record := RollbackRecord{
		txNum: txNum,
	}
	assert.Equal(t, txNum, record.TxNum())
}

func TestRollbackRecordUndo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	const txNum = 1
	record := RollbackRecord{
		txNum: txNum,
	}
	record.Undo(nil)
}

func TestRollbackRecordString(t *testing.T) {
	const txNum = 1
	record := RollbackRecord{
		txNum: txNum,
	}
	assert.Equal(t, "<ROLLBACK 1>", record.String())
}

func TestWriteRollbackRecordToLog(t *testing.T) {
	const (
		txNum = 1
		lsn   = 2
	)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	lm := lmock.NewMockLogMgr(ctrl)
	lm.EXPECT().Append([]byte{
		0, 0, 0, 3, // ROLLBACK
		0, 0, 0, 1, // txNum
	}).Return(lsn, nil)

	got, err := WriteRollbackRecordToLog(lm, txNum)

	assert.NoError(t, err)
	assert.Equal(t, lsn, got)
}
