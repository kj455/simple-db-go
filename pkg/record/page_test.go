package record

import (
	"fmt"
	"testing"

	"github.com/kj455/db/pkg/buffer"
	buffermgr "github.com/kj455/db/pkg/buffer_mgr"
	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/testutil"
	"github.com/kj455/db/pkg/tx/transaction"
	"github.com/stretchr/testify/assert"
)

var randInts = []int{38, 1, 31, 13, 30, 4, 16, 47, 29, 33}

func TestRecord(t *testing.T) {
	rootDir := testutil.ProjectRootDir()
	dir := rootDir + "/.tmp"
	fm := file.NewFileMgr(dir, 400)
	lm, _ := log.NewLogMgr(fm, "testlogfile")
	buffs := make([]buffer.Buffer, 2)
	for i := range buffs {
		buffs[i] = buffer.NewBuffer(fm, lm, 400)
	}
	bm := buffermgr.NewBufferMgr(buffs)
	txNumGen := transaction.NewTxNumberGenerator()
	tx, _ := transaction.NewTransaction(fm, lm, bm, txNumGen)

	sch := NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	layout, _ := NewLayoutFromSchema(sch)
	assert.Equal(t, 4, layout.Offset("A"))
	assert.Equal(t, 8, layout.Offset("B"))

	for _, fldname := range layout.Schema().Fields() {
		offset := layout.Offset(fldname)
		t.Logf("%s has offset %d\n", fldname, offset)
	}

	blk, _ := tx.Append("testfile")
	tx.Pin(blk)
	rp, _ := NewRecordPage(tx, blk, layout)
	rp.Format()

	t.Logf("Filling the page with random records.")
	slot, _ := rp.InsertAfter(-1)
	for slot >= 0 {
		n := randInts[slot]
		rp.SetInt(slot, "A", n)
		rp.SetString(slot, "B", "rec"+fmt.Sprintf("%d", n))
		t.Logf("inserting into slot %d: {%d, rec%d}\n", slot, n, n)
		slot, _ = rp.InsertAfter(slot)
	}

	t.Logf("Deleting these records, whose A-values are less than 30.")
	t.Logf("page has %d slots\n", layout.SlotSize())
	count := 0
	slot = rp.NextAfter(-1)
	for slot >= 0 {
		a, _ := rp.GetInt(slot, "A")
		b, _ := rp.GetString(slot, "B")
		if a < 30 {
			count++
			t.Logf("slot %d: {%d, %s}\n", slot, a, b)
			rp.Delete(slot)
		}
		slot = rp.NextAfter(slot)
	}
	t.Logf("page has %d slots\n", layout.SlotSize())

	t.Logf("Here are the remaining records.")
	slot = rp.NextAfter(-1)
	for slot >= 0 {
		a, _ := rp.GetInt(slot, "A")
		b, _ := rp.GetString(slot, "B")
		t.Logf("slot %d: {%d, %s}\n", slot, a, b)
		assert.Equal(t, randInts[slot], a)
		assert.Equal(t, "rec"+fmt.Sprintf("%d", randInts[slot]), b)
		slot = rp.NextAfter(slot)
	}
	tx.Unpin(blk)
	tx.Commit()
}
