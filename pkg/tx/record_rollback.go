package tx

import (
	"fmt"

	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/log"
)

type RollbackRecord struct {
	txNum int
}

func NewRollbackRecord(p file.Page) *RollbackRecord {
	tpos := OffsetTxNum
	txNum := p.GetInt(tpos)
	return &RollbackRecord{
		txNum: int(txNum),
	}
}

func (r *RollbackRecord) Op() Op {
	return OP_ROLLBACK
}

func (r *RollbackRecord) TxNum() int {
	return r.txNum
}

func (r *RollbackRecord) Undo(tx Transaction) error {
	return nil
}

func (r *RollbackRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %d>", r.txNum)
}

func WriteRollbackRecordToLog(lm log.LogMgr, txNum int) (int, error) {
	const txNumSize = 4
	length := OffsetTxNum + txNumSize
	record := make([]byte, length)
	p := file.NewPageFromBytes(record)
	p.SetInt(0, uint32(OP_ROLLBACK))
	p.SetInt(OffsetTxNum, uint32(txNum))
	return lm.Append(record)
}
