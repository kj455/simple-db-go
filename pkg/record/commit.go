package record

import (
	"fmt"

	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/tx"
)

type CommitRecord struct {
	txNum int
}

func NewCommitRecord(p file.Page) *CommitRecord {
	tpos := OpSize
	txNum := p.GetInt(tpos)
	return &CommitRecord{
		txNum: int(txNum),
	}
}

func (r *CommitRecord) Op() Op {
	return COMMIT
}

func (r *CommitRecord) TxNum() int {
	return r.txNum
}

func (r *CommitRecord) Undo(tx tx.Transaction) {
	// no-op
}

func (r *CommitRecord) String() string {
	return fmt.Sprintf("<COMMIT %d>", r.txNum)
}

func CommitRecordWriteToLog(lm log.LogMgr, txNum int) (int, error) {
	const txNumSize = 4
	record := make([]byte, OpSize+txNumSize)
	p := file.NewPageFromBytes(record)
	p.SetInt(0, uint32(COMMIT))
	p.SetInt(4, uint32(txNum))
	return lm.Append(record)
}
