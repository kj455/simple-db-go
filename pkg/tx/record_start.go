package tx

import (
	"fmt"

	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/log"
)

type StartRecord struct {
	txNum int
}

func NewStartRecord(p file.Page) *StartRecord {
	tpos := OffsetTxNum
	txNum := p.GetInt(tpos)
	return &StartRecord{
		txNum: int(txNum),
	}
}

func (r *StartRecord) Op() Op {
	return OP_START
}

func (r *StartRecord) TxNum() int {
	return r.txNum
}

func (r *StartRecord) Undo(tx Transaction) error {
	return nil
}

func (r *StartRecord) String() string {
	return fmt.Sprintf("<START %d>", r.txNum)
}

func WriteStartRecordToLog(lm log.LogMgr, txNum int) (lsn int, err error) {
	const txNumSize = 4
	record := make([]byte, OffsetTxNum+txNumSize)
	p := file.NewPageFromBytes(record)
	p.SetInt(OffsetOp, uint32(OP_START))
	p.SetInt(OffsetTxNum, uint32(txNum))
	return lm.Append(record)
}
