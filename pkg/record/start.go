package record

import (
	"fmt"

	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/tx"
)

type StartRecord struct {
	txNum int
}

func NewStartRecord(p file.Page) *StartRecord {
	tpos := OpSize
	txNum := p.GetInt(tpos)
	return &StartRecord{
		txNum: int(txNum),
	}
}

func (r *StartRecord) Op() Op {
	return START
}

func (r *StartRecord) TxNum() int {
	return r.txNum
}

func (r *StartRecord) Undo(tx tx.Transaction) {
	// no-op
}

func (r *StartRecord) String() string {
	return fmt.Sprintf("<START %d>", r.txNum)
}

func WriteStartRecordToLog(lm log.LogMgr, txNum int) (int, error) {
	const txNumSize = 4
	record := make([]byte, OpSize+txNumSize)
	p := file.NewPageFromBytes(record)
	p.SetInt(0, uint32(START))
	p.SetInt(OpSize, uint32(txNum))
	return lm.Append(record)
}
