package transaction

import (
	"testing"

	lmock "github.com/kj455/db/pkg/log/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestNewCheckpointRecord(t *testing.T) {
	record := NewCheckpointRecord()
	assert.NotNil(t, record)
}

func TestCheckpointRecordOp(t *testing.T) {
	record := CheckpointRecord{}
	assert.Equal(t, CHECKPOINT, record.Op())
}

func TestCheckpointRecordTxNum(t *testing.T) {
	record := CheckpointRecord{}
	assert.Equal(t, dummyTxNum, record.TxNum())
}

func TestCheckpointRecordUndo(t *testing.T) {
	record := CheckpointRecord{}
	record.Undo(nil)
}

func TestCheckpointRecordString(t *testing.T) {
	record := CheckpointRecord{}
	assert.Equal(t, "<CHECKPOINT>", record.String())
}

func TestWriteCheckpointRecordToLog(t *testing.T) {
	const (
		txNum = 1
		lsn   = 2
	)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	lm := lmock.NewMockLogMgr(ctrl)
	lm.EXPECT().Append([]byte{
		0, 0, 0, 0, // CHECKPOINT
	}).Return(lsn, nil)

	got, err := WriteCheckpointRecordToLog(lm)
	assert.NoError(t, err)
	assert.Equal(t, lsn, got)
}
