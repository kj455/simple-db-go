package record

import (
	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/tx"
)

type SlotFlag int

const (
	SLOT_EMPTY SlotFlag = 0
	SLOT_USED  SlotFlag = 1
)

const SLOT_INIT = -1

/*
RecordPage represents a page of records in a file.
The format of the page is as follows:
----------------------------------------
| slot 0 | slot 1 | ... | slot n | ... |
| 1 | R0 | 0 | R1 | ... | 0 | Rx | ... |
----------------------------------------
*/
type RecordPageImpl struct {
	tx     tx.Transaction
	blk    file.BlockId
	layout Layout
}

func NewRecordPage(t tx.Transaction, b file.BlockId, l Layout) (RecordPage, error) {
	if err := t.Pin(b); err != nil {
		return nil, err
	}
	return &RecordPageImpl{
		tx:     t,
		blk:    b,
		layout: l,
	}, nil
}

func (rp *RecordPageImpl) GetInt(slot int, field string) (int, error) {
	pos := rp.offset(slot) + rp.layout.Offset(field)
	return rp.tx.GetInt(rp.blk, pos)
}

func (rp *RecordPageImpl) GetString(slot int, field string) (string, error) {
	pos := rp.offset(slot) + rp.layout.Offset(field)
	return rp.tx.GetString(rp.blk, pos)
}

func (rp *RecordPageImpl) SetInt(slot int, field string, val int) error {
	pos := rp.offset(slot) + rp.layout.Offset(field)
	return rp.tx.SetInt(rp.blk, pos, val, true)
}

func (rp *RecordPageImpl) SetString(slot int, field string, val string) error {
	pos := rp.offset(slot) + rp.layout.Offset(field)
	return rp.tx.SetString(rp.blk, pos, val, true)
}

func (rp *RecordPageImpl) Delete(slot int) error {
	return rp.setFlag(slot, SLOT_EMPTY)
}

// Format initializes all the slots on the page to empty.
func (rp *RecordPageImpl) Format() error {
	slot := 0
	schema := rp.layout.Schema()
	for rp.isValidSlot(slot) {
		if err := rp.tx.SetInt(rp.blk, rp.offset(slot), int(SLOT_EMPTY), false); err != nil {
			return err
		}
		for _, field := range schema.Fields() {
			pos := rp.offset(slot) + rp.layout.Offset(field)
			typ, err := schema.Type(field)
			if err != nil {
				return err
			}
			switch typ {
			case SCHEMA_TYPE_INTEGER:
				if err := rp.tx.SetInt(rp.blk, pos, 0, false); err != nil {
					return err
				}
			case SCHEMA_TYPE_VARCHAR:
				if err := rp.tx.SetString(rp.blk, pos, "", false); err != nil {
					return err
				}
			}
		}
		slot++
	}
	return nil
}

// NextAfter returns the next slot after the given slot.
// If no such slot is found, it returns -1.
func (rp *RecordPageImpl) NextAfter(slot int) int {
	return rp.searchAfter(slot, SLOT_USED)
}

// InsertAfter inserts a new record after the given slot.
// If no such slot is found, it returns -1.
func (rp *RecordPageImpl) InsertAfter(slot int) (int, error) {
	newSlot := rp.searchAfter(slot, SLOT_EMPTY)
	if newSlot >= 0 {
		if err := rp.setFlag(newSlot, SLOT_USED); err != nil {
			return SLOT_INIT, err
		}
	}
	return newSlot, nil
}

func (rp *RecordPageImpl) Block() file.BlockId {
	return rp.blk
}

func (rp *RecordPageImpl) setFlag(slot int, flag SlotFlag) error {
	return rp.tx.SetInt(rp.blk, rp.offset(slot), int(flag), true)
}

// searchAfter finds the next slot with the given flag.
// If no such slot is found, it returns -1.
func (rp *RecordPageImpl) searchAfter(slot int, flag SlotFlag) int {
	sl := slot + 1
	for rp.isValidSlot(sl) {
		val, _ := rp.tx.GetInt(rp.blk, rp.offset(sl))
		if val == int(flag) {
			return sl
		}
		sl++
	}
	return SLOT_INIT
}

func (rp *RecordPageImpl) isValidSlot(slot int) bool {
	return rp.offset(slot+1) <= rp.tx.BlockSize()
}

func (rp *RecordPageImpl) offset(slot int) int {
	return slot * rp.layout.SlotSize()
}
