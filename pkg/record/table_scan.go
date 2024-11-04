package record

import (
	"fmt"

	"github.com/kj455/db/pkg/constant"
	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/tx"
)

type TableScanImpl struct {
	tx         tx.Transaction
	layout     Layout
	recordPage RecordPage
	filename   string
	curSlot    int
}

const TABLE_SUFFIX = ".tbl"

func NewTableScan(tx tx.Transaction, table string, layout Layout) (*TableScanImpl, error) {
	ts := &TableScanImpl{
		tx:       tx,
		layout:   layout,
		filename: table + TABLE_SUFFIX,
	}
	size, err := tx.Size(ts.filename)
	if err != nil {
		return nil, fmt.Errorf("record: failed to get size of %s: %w", ts.filename, err)
	}
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
	ts.curSlot = ts.recordPage.NextAfter(ts.curSlot)
	for ts.curSlot < 0 {
		if ts.atLastBlock() {
			return false
		}
		err := ts.moveToBlock(ts.recordPage.Block().Number() + 1)
		if err != nil {
			fmt.Println("record: table scan: next: ", err)
			return false
		}
		ts.curSlot = ts.recordPage.NextAfter(ts.curSlot)
	}
	return true
}

func (ts *TableScanImpl) GetInt(field string) (int, error) {
	return ts.recordPage.GetInt(ts.curSlot, field)
}

func (ts *TableScanImpl) GetString(field string) (string, error) {
	return ts.recordPage.GetString(ts.curSlot, field)
}

func (ts *TableScanImpl) GetVal(field string) (*constant.Const, error) {
	schemaType, err := ts.layout.Schema().Type(field)
	if err != nil {
		return nil, fmt.Errorf("record: failed to get type: %w", err)
	}
	switch schemaType {
	case SCHEMA_TYPE_INTEGER:
		v, err := ts.GetInt(field)
		if err != nil {
			return nil, err
		}
		return constant.NewConstant(constant.KIND_INT, v)
	case SCHEMA_TYPE_VARCHAR:
		v, err := ts.GetString(field)
		if err != nil {
			return nil, err
		}
		return constant.NewConstant(constant.KIND_STR, v)
	default:
		return nil, fmt.Errorf("record: unknown schema type %v", schemaType)
	}
}

func (ts *TableScanImpl) HasField(field string) bool {
	return ts.layout.Schema().HasField(field)
}

func (ts *TableScanImpl) Close() {
	if ts.recordPage != nil {
		ts.tx.Unpin(ts.recordPage.Block())
	}
}

func (ts *TableScanImpl) SetInt(field string, val int) error {
	fmt.Println("field:", field, "val:", val, ts.curSlot)
	return ts.recordPage.SetInt(ts.curSlot, field, val)
}

func (ts *TableScanImpl) SetString(field string, val string) error {
	return ts.recordPage.SetString(ts.curSlot, field, val)
}

func (ts *TableScanImpl) SetVal(field string, val *constant.Const) error {
	schemaType, err := ts.layout.Schema().Type(field)
	if err != nil {
		return err
	}
	switch schemaType {
	case SCHEMA_TYPE_INTEGER:
		val, err := val.AsInt()
		if err != nil {
			return fmt.Errorf("record: failed to convert val to int: %w", err)
		}
		return ts.SetInt(field, val)
	case SCHEMA_TYPE_VARCHAR:
		val, err := val.AsString()
		if err != nil {
			return fmt.Errorf("record: failed to convert val to string: %w", err)
		}
		return ts.SetString(field, val)
	}
	return nil
}

func (ts *TableScanImpl) Insert() error {
	var err error
	ts.curSlot, err = ts.recordPage.InsertAfter(ts.curSlot)
	if err != nil {
		return fmt.Errorf("record: table scan: insert: %w", err)
	}
	for ts.curSlot < 0 {
		if ts.atLastBlock() {
			err = ts.moveToNewBlock()
		} else {
			err = ts.moveToBlock(ts.recordPage.Block().Number() + 1)
		}
		if err != nil {
			return fmt.Errorf("record: table scan: insert: %w", err)
		}
		ts.curSlot, err = ts.recordPage.InsertAfter(ts.curSlot)
		if err != nil {
			return fmt.Errorf("record: table scan: insert: %w", err)
		}
	}
	return nil
}

func (ts *TableScanImpl) Delete() error {
	return ts.recordPage.Delete(ts.curSlot)
}

func (ts *TableScanImpl) MoveToRid(rid RID) {
	ts.Close()
	blk := file.NewBlockId(ts.filename, rid.BlockNumber())
	ts.recordPage, _ = NewRecordPage(ts.tx, blk, ts.layout)
	ts.curSlot = rid.Slot()
}

func (ts *TableScanImpl) GetRid() RID {
	return NewRID(ts.recordPage.Block().Number(), ts.curSlot)
}

func (ts *TableScanImpl) moveToBlock(blknum int) (err error) {
	ts.Close()
	blk := file.NewBlockId(ts.filename, blknum)
	ts.recordPage, err = NewRecordPage(ts.tx, blk, ts.layout)
	if err != nil {
		return err
	}
	ts.curSlot = SLOT_INIT
	return nil
}

func (ts *TableScanImpl) moveToNewBlock() error {
	ts.Close()
	blk, err := ts.tx.Append(ts.filename)
	if err != nil {
		return err
	}
	ts.recordPage, err = NewRecordPage(ts.tx, blk, ts.layout)
	if err != nil {
		return err
	}
	if err := ts.recordPage.Format(); err != nil {
		return err
	}
	ts.curSlot = SLOT_INIT
	return nil
}

func (ts *TableScanImpl) atLastBlock() bool {
	size, _ := ts.tx.Size(ts.filename)
	return ts.recordPage.Block().Number() == size-1
}
