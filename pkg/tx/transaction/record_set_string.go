package transaction

import (
	"fmt"

	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/log"
	"github.com/kj455/simple-db/pkg/tx"
)

/*
------------------------------------------------
|   0  |   4   |  8   | n     | n+4    | n+8  |
------------------------------------------------
|  op  | txNum | file | block | offset | value |
------------------------------------------------
*/
type SetStringRecord struct {
	txNum  int
	offset int
	val    string
	block  file.BlockId
}

func NewSetStringRecord(p file.Page) *SetStringRecord {
	const biteSize = 4
	txPos := OffsetTxNum
	txNum := p.GetInt(txPos)
	fnPos := txPos + biteSize
	filename := p.GetString(fnPos)
	blkPos := fnPos + file.MaxLength(len(filename))
	blkNum := p.GetInt(blkPos)
	block := file.NewBlockId(filename, int(blkNum))
	offPos := blkPos + biteSize
	offset := p.GetInt(offPos)
	valPos := offPos + biteSize
	val := p.GetString(valPos)
	return &SetStringRecord{
		txNum:  int(txNum),
		offset: int(offset),
		val:    val,
		block:  block,
	}
}

func (r *SetStringRecord) Op() Op {
	return OP_SET_STRING
}

func (r *SetStringRecord) TxNum() int {
	return r.txNum
}

func (r *SetStringRecord) Undo(tx tx.Transaction) error {
	if err := tx.Pin(r.block); err != nil {
		return err
	}
	// don't log the undo
	if err := tx.SetString(r.block, r.offset, r.val, false); err != nil {
		return err
	}
	tx.Unpin(r.block)
	return nil
}

func (r *SetStringRecord) String() string {
	return fmt.Sprintf("<SET_STRING %d %s %d %s>", r.txNum, r.block, r.offset, r.val)
}

func WriteSetStringRecordToLog(lm log.LogMgr, txNum int, block file.BlockId, offset int, val string) (int, error) {
	tpos := OffsetTxNum
	fpos := tpos + 4
	bpos := fpos + file.MaxLength(len(block.Filename()))
	opos := bpos + 4
	vpos := opos + 4
	recordLen := vpos + file.MaxLength(len(val))
	record := make([]byte, recordLen)
	p := file.NewPageFromBytes(record)
	p.SetInt(0, uint32(OP_SET_STRING))
	p.SetInt(tpos, uint32(txNum))
	p.SetString(fpos, block.Filename())
	p.SetInt(bpos, uint32(block.Number()))
	p.SetInt(opos, uint32(offset))
	p.SetString(vpos, val)
	return lm.Append(record)
}
