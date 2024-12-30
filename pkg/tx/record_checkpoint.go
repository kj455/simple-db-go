package tx

import (
	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/log"
)

const dummyTxNum = -1

type CheckpointRecord struct{}

func NewCheckpointRecord() *CheckpointRecord {
	return &CheckpointRecord{}
}

func (r *CheckpointRecord) Op() Op {
	return OP_CHECKPOINT
}

func (r *CheckpointRecord) TxNum() int {
	return dummyTxNum
}

func (r *CheckpointRecord) Undo(tx Transaction) error {
	return nil
}

func (r *CheckpointRecord) String() string {
	return "<CHECKPOINT>"
}

func WriteCheckpointRecordToLog(lm log.LogMgr) (int, error) {
	record := make([]byte, OffsetTxNum)
	p := file.NewPageFromBytes(record)
	p.SetInt(0, uint32(OP_CHECKPOINT))
	return lm.Append(record)
}
