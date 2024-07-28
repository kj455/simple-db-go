package record

import (
	"testing"

	"github.com/kj455/db/pkg/buffer"
	buffermgr "github.com/kj455/db/pkg/buffer_mgr"
	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/testutil"
	"github.com/kj455/db/pkg/tx/transaction"
)

func TestTableScan(t *testing.T) {
	const blockSize = 400
	rootDir := testutil.ProjectRootDir()
	dir := rootDir + "/.tmp"
	fm := file.NewFileMgr(dir, blockSize)
	lm, _ := log.NewLogMgr(fm, "testlogfile")
	buffs := make([]buffer.Buffer, 2)
	for i := range buffs {
		buffs[i] = buffer.NewBuffer(fm, lm, blockSize)
	}
	bm := buffermgr.NewBufferMgr(buffs)
	txNumGen := transaction.NewTxNumberGenerator()
	tx, _ := transaction.NewTransaction(fm, lm, bm, txNumGen)

	sch := NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	layout, _ := NewLayoutFromSchema(sch)

	for _, fldname := range layout.Schema().Fields() {
		offset := layout.Offset(fldname)
		t.Logf("%s has offset %d\n", fldname, offset)
	}

	t.Logf("table has %d slots\n", layout.SlotSize())
	scan, _ := NewTableScan(tx, "T", layout)

	count := 0
	scan.BeforeFirst()
	for scan.Next() {
		a, _ := scan.GetInt("A")
		b, _ := scan.GetString("B")
		if a < 25 {
			count++
			t.Logf("slot %s: {%d, %s}\n", scan.GetRid(), a, b)
			scan.Delete()
		}
	}
	t.Logf("table has %d slots\n", layout.SlotSize())

	t.Logf("Here are the remaining records.")
	scan.BeforeFirst()
	for scan.Next() {
		a, _ := scan.GetInt("A")
		b, _ := scan.GetString("B")
		t.Logf("slot %s: {%d, %s}\n", scan.GetRid(), a, b)
	}
	scan.Close()
	tx.Commit()

	// t.Error()
}
