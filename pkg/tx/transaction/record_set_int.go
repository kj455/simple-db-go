package transaction

import (
	"fmt"

	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/tx"
)

type SetIntRecord struct {
	txNum  int
	offset int
	val    int
	block  file.BlockId
}

func NewSetIntRecord(p file.Page) *SetIntRecord {
	tpos := OpSize
	txnum := p.GetInt(tpos)
	fnPos := tpos + 4
	filename := p.GetString(fnPos)
	bnPos := fnPos + file.MaxLength(len(filename))
	blockNum := p.GetInt(bnPos)
	block := file.NewBlockId(filename, int(blockNum))
	offPos := bnPos + 4
	offset := p.GetInt(offPos)
	valPos := offPos + 4
	val := p.GetInt(valPos)
	return &SetIntRecord{
		txNum:  int(txnum),
		offset: int(offset),
		val:    int(val),
		block:  block,
	}
}

func (r *SetIntRecord) Op() Op {
	return SET_INT
}

func (r *SetIntRecord) TxNum() int {
	return r.txNum
}

func (r *SetIntRecord) Undo(tx tx.Transaction) error {
	if err := tx.Pin(r.block); err != nil {
		return err
	}
	if err := tx.SetInt(r.block, r.offset, r.val, false); err != nil {
		return err
	}
	tx.Unpin(r.block)
	return nil
}

func (r *SetIntRecord) String() string {
	return fmt.Sprintf("<SET_INT %d %s %d %d>", r.txNum, r.block, r.offset, r.val)
}

func WriteSetIntRecordToLog(lm log.LogMgr, txNum int, block file.BlockId, offset int, val int) (int, error) {
	tpos := 4
	fnPos := tpos + 4
	bnPos := fnPos + file.MaxLength(len(block.Filename()))
	offPos := bnPos + 4
	valPos := offPos + 4
	rec := make([]byte, valPos+4)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, uint32(SET_INT))
	p.SetInt(tpos, uint32(txNum))
	p.SetString(fnPos, block.Filename())
	p.SetInt(bnPos, uint32(block.Number()))
	p.SetInt(offPos, uint32(offset))
	p.SetInt(valPos, uint32(val))
	return lm.Append(rec)
}
