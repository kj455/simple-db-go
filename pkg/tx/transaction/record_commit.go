package transaction

import (
	"fmt"

	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/log"
	"github.com/kj455/simple-db/pkg/tx"
)

type CommitRecord struct {
	txNum int
}

func NewCommitRecord(p file.Page) *CommitRecord {
	tpos := OffsetTxNum
	txNum := p.GetInt(tpos)
	return &CommitRecord{
		txNum: int(txNum),
	}
}

func (r *CommitRecord) Op() Op {
	return OP_COMMIT
}

func (r *CommitRecord) TxNum() int {
	return r.txNum
}

func (r *CommitRecord) Undo(tx tx.Transaction) error {
	return nil
}

func (r *CommitRecord) String() string {
	return fmt.Sprintf("<COMMIT %d>", r.txNum)
}

func WriteCommitRecordToLog(lm log.LogMgr, txNum int) (int, error) {
	const txNumSize = 4
	record := make([]byte, OffsetTxNum+txNumSize)
	p := file.NewPageFromBytes(record)
	p.SetInt(0, uint32(OP_COMMIT))
	p.SetInt(4, uint32(txNum))
	return lm.Append(record)
}
