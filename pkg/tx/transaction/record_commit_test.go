package transaction

import (
	"testing"

	fmock "github.com/kj455/db/pkg/file/mock"
	lmock "github.com/kj455/db/pkg/log/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestNewCommitRecord(t *testing.T) {
	const txNum = 1
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	page := fmock.NewMockPage(ctrl)
	page.EXPECT().GetInt(OpSize).Return(uint32(txNum))

	record := NewCommitRecord(page)

	assert.Equal(t, COMMIT, record.Op())
	assert.Equal(t, txNum, record.TxNum())
}

func TestCommitRecordOp(t *testing.T) {
	record := CommitRecord{}
	assert.Equal(t, COMMIT, record.Op())
}

func TestCommitRecordTxNum(t *testing.T) {
	const txNum = 1
	record := CommitRecord{
		txNum: txNum,
	}
	assert.Equal(t, txNum, record.TxNum())
}

func TestCommitRecordUndo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	const txNum = 1
	record := CommitRecord{
		txNum: txNum,
	}
	record.Undo(nil)
}

func TestCommitRecordString(t *testing.T) {
	const txNum = 1
	record := CommitRecord{
		txNum: txNum,
	}
	assert.Equal(t, "<COMMIT 1>", record.String())
}

func TestWriteCommitRecordToLog(t *testing.T) {
	const (
		txNum = 1
		lsn   = 2
	)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	lm := lmock.NewMockLogMgr(ctrl)
	lm.EXPECT().Append([]byte{
		0, 0, 0, 2, // COMMIT
		0, 0, 0, 1, // txNum
	}).Return(lsn, nil)

	got, err := WriteCommitRecordToLog(lm, txNum)

	assert.NoError(t, err)
	assert.Equal(t, lsn, got)
}
