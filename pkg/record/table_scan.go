package record

import (
	"fmt"

	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/tx"
)

type TableScanImpl struct {
	tx       tx.Transaction
	layout   Layout
	rp       RecordPage
	filename string
	curSlot  int
}

const TABLE_SUFFIX = ".tbl"

func NewTableScan(tx tx.Transaction, table string, layout Layout) (TableScan, error) {
	ts := &TableScanImpl{
		tx:       tx,
		layout:   layout,
		filename: table + TABLE_SUFFIX,
	}
	size, _ := tx.Size(ts.filename)
	var err error
	if size == 0 {
		err = ts.moveToNewBlock()
	} else {
		err = ts.moveToBlock(0)
	}
	if err != nil {
		return nil, fmt.Errorf("record: new table scan: %w", err)
	}
	return ts, nil
}

func (ts *TableScanImpl) BeforeFirst() error {
	return ts.moveToBlock(0)
}

func (ts *TableScanImpl) Next() bool {
	ts.curSlot = ts.rp.NextAfter(ts.curSlot)
	for ts.curSlot < 0 {
		if ts.atLastBlock() {
			return false
		}
		err := ts.moveToBlock(ts.rp.Block().Number() + 1)
		if err != nil {
			fmt.Println("record: table scan: next: ", err)
			return false
		}
		ts.curSlot = ts.rp.NextAfter(ts.curSlot)
	}
	return true
}

func (ts *TableScanImpl) GetInt(field string) (int, error) {
	return ts.rp.GetInt(ts.curSlot, field)
}

func (ts *TableScanImpl) GetString(field string) (string, error) {
	return ts.rp.GetString(ts.curSlot, field)
}

func (ts *TableScanImpl) GetVal(field string) (any, error) {
	schemaType, err := ts.layout.Schema().Type(field)
	if err != nil {
		return nil, err
	}
	switch schemaType {
	case SCHEMA_TYPE_INTEGER:
		return ts.GetInt(field)
	case SCHEMA_TYPE_VARCHAR:
		return ts.GetString(field)
	default:
		return nil, fmt.Errorf("record: table scan: get val: unknown type %v", schemaType)
	}
}

func (ts *TableScanImpl) HasField(field string) bool {
	return ts.layout.Schema().HasField(field)
}

func (ts *TableScanImpl) Close() {
	if ts.rp != nil {
		ts.tx.Unpin(ts.rp.Block())
	}
}

func (ts *TableScanImpl) SetInt(field string, val int) error {
	return ts.rp.SetInt(ts.curSlot, field, val)
}

func (ts *TableScanImpl) SetString(field string, val string) error {
	return ts.rp.SetString(ts.curSlot, field, val)
}

func (ts *TableScanImpl) SetVal(field string, val any) error {
	schemaType, err := ts.layout.Schema().Type(field)
	if err != nil {
		return err
	}
	switch schemaType {
	case SCHEMA_TYPE_INTEGER:
		v, ok := val.(int)
		if !ok {
			return fmt.Errorf("record: table scan: set val: expected int, got %v", val)
		}
		return ts.SetInt(field, v)
	case SCHEMA_TYPE_VARCHAR:
		v, ok := val.(string)
		if !ok {
			return fmt.Errorf("record: table scan: set val: expected string, got %v", val)
		}
		return ts.SetString(field, v)
	}
	return nil
}

func (ts *TableScanImpl) Insert() error {
	var err error
	ts.curSlot, err = ts.rp.InsertAfter(ts.curSlot)
	if err != nil {
		return fmt.Errorf("record: table scan: insert: %w", err)
	}
	for ts.curSlot < 0 {
		if ts.atLastBlock() {
			err = ts.moveToNewBlock()
		} else {
			err = ts.moveToBlock(ts.rp.Block().Number() + 1)
		}
		if err != nil {
			return fmt.Errorf("record: table scan: insert: %w", err)
		}
		ts.curSlot, err = ts.rp.InsertAfter(ts.curSlot)
		if err != nil {
			return fmt.Errorf("record: table scan: insert: %w", err)
		}
	}
	return nil
}

func (ts *TableScanImpl) Delete() error {
	return ts.rp.Delete(ts.curSlot)
}

func (ts *TableScanImpl) MoveToRid(rid RID) {
	ts.Close()
	blk := file.NewBlockId(ts.filename, rid.BlockNumber())
	ts.rp, _ = NewRecordPage(ts.tx, blk, ts.layout)
	ts.curSlot = rid.Slot()
}

func (ts *TableScanImpl) GetRid() RID {
	return NewRID(ts.rp.Block().Number(), ts.curSlot)
}

func (ts *TableScanImpl) moveToBlock(blknum int) (err error) {
	ts.Close()
	blk := file.NewBlockId(ts.filename, blknum)
	ts.rp, err = NewRecordPage(ts.tx, blk, ts.layout)
	if err != nil {
		return err
	}
	ts.curSlot = -1
	return nil
}

func (ts *TableScanImpl) moveToNewBlock() error {
	ts.Close()
	blk, err := ts.tx.Append(ts.filename)
	if err != nil {
		return err
	}
	ts.rp, err = NewRecordPage(ts.tx, blk, ts.layout)
	if err != nil {
		return err
	}
	if err := ts.rp.Format(); err != nil {
		return err
	}
	ts.curSlot = -1
	return nil
}

func (ts *TableScanImpl) atLastBlock() bool {
	size, _ := ts.tx.Size(ts.filename)
	return ts.rp.Block().Number() == size-1
}
