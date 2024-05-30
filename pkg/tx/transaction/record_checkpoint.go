package transaction

import (
	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/tx"
)

const dummyTxNum = -1

type CheckpointRecord struct{}

func NewCheckpointRecord() *CheckpointRecord {
	return &CheckpointRecord{}
}

func (r *CheckpointRecord) Op() Op {
	return CHECKPOINT
}

func (r *CheckpointRecord) TxNum() int {
	return dummyTxNum
}

func (r *CheckpointRecord) Undo(tx tx.Transaction) {
	// no-op
}

func (r *CheckpointRecord) String() string {
	return "<CHECKPOINT>"
}

func WriteCheckpointRecordToLog(lm log.LogMgr) (int, error) {
	record := make([]byte, OpSize)
	p := file.NewPageFromBytes(record)
	p.SetInt(0, uint32(CHECKPOINT))
	return lm.Append(record)
}
