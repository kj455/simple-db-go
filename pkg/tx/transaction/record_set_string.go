package transaction

import (
	"fmt"

	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/tx"
)

type SetStringRecord struct {
	txNum  int
	offset int
	val    string
	block  file.BlockId
}

func NewSetStringRecord(p file.Page) *SetStringRecord {
	txPos := OpSize
	txNum := p.GetInt(txPos)
	fnPos := txPos + 4
	filename := p.GetString(fnPos)
	blkPos := fnPos + file.MaxLength(len(filename))
	blockNum := p.GetInt(blkPos)
	block := file.NewBlockId(filename, int(blockNum))
	offPos := blkPos + 4
	offset := p.GetInt(offPos)
	valPos := offPos + 4
	val := p.GetString(valPos)
	return &SetStringRecord{
		txNum:  int(txNum),
		offset: int(offset),
		val:    val,
		block:  block,
	}
}

func (r *SetStringRecord) Op() Op {
	return SET_STRING
}

func (r *SetStringRecord) TxNum() int {
	return r.txNum
}

func (r *SetStringRecord) Undo(tx tx.Transaction) {
	tx.Pin(r.block)
	tx.SetString(r.block, r.offset, r.val, false) // false: don't log the undo
	tx.Unpin(r.block)
}

func (r *SetStringRecord) String() string {
	return fmt.Sprintf("<SET_STRING %d %s %d %s>", r.txNum, r.block, r.offset, r.val)
}

func WriteSetStringRecordToLog(lm log.LogMgr, txNum int, block file.BlockId, offset int, val string) (int, error) {
	tpos := OpSize
	fpos := tpos + 4
	bpos := fpos + file.MaxLength(len(block.Filename()))
	opos := bpos + 4
	vpos := opos + 4
	recordLen := vpos + file.MaxLength(len(val))
	record := make([]byte, recordLen)
	p := file.NewPageFromBytes(record)
	p.SetInt(0, uint32(SET_STRING))
	p.SetInt(tpos, uint32(txNum))
	p.SetString(fpos, block.Filename())
	p.SetInt(bpos, uint32(block.Number()))
	p.SetInt(opos, uint32(offset))
	p.SetString(vpos, val)
	return lm.Append(record)
}
