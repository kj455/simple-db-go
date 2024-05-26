package record

import (
	"fmt"

	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/tx"
)

type RollbackRecord struct {
	txNum int
}

func NewRollbackRecord(p file.Page) *RollbackRecord {
	tpos := OpSize
	txNum := p.GetInt(tpos)
	return &RollbackRecord{
		txNum: int(txNum),
	}
}

func (r *RollbackRecord) Op() Op {
	return ROLLBACK
}

func (r *RollbackRecord) TxNum() int {
	return r.txNum
}

func (r *RollbackRecord) Undo(tx tx.Transaction) {
	// no-op
}

func (r *RollbackRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %d>", r.txNum)
}

func WriteRollbackRecordToLog(lm log.LogMgr, txNum int) (int, error) {
	const txNumSize = 4
	length := OpSize + txNumSize
	record := make([]byte, length)
	p := file.NewPageFromBytes(record)
	p.SetInt(0, uint32(ROLLBACK))
	p.SetInt(OpSize, uint32(txNum))
	return lm.Append(record)
}
