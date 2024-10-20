package metadata

import (
	// "fmt"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/kj455/db/pkg/buffer"
	buffermgr "github.com/kj455/db/pkg/buffer_mgr"
	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/record"
	"github.com/kj455/db/pkg/testutil"
	"github.com/kj455/db/pkg/tx/transaction"
)

func TestMetadata(t *testing.T) {
	t.Skip() // TODO: fix this test
	rootDir := testutil.RootDir()
	dir := rootDir + "/.tmp"
	fm := file.NewFileMgr(dir, 800)
	lm, _ := log.NewLogMgr(fm, "testlogfile")
	buffs := make([]buffer.Buffer, 2)
	for i := range buffs {
		buffs[i] = buffer.NewBuffer(fm, lm, 800)
	}
	bm := buffermgr.NewBufferMgr(buffs)
	txNumGen := transaction.NewTxNumberGenerator()
	tx, _ := transaction.NewTransaction(fm, lm, bm, txNumGen)
	mdm, _ := NewMetadataMgr(true, tx)

	sch := record.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)

	// Part 1: Table Metadata
	mdm.CreateTable("MyTable", sch, tx)
	layout, _ := mdm.GetLayout("MyTable", tx)
	size := layout.SlotSize()
	sch2 := layout.Schema()
	t.Logf("MyTable has slot size %d\n", size)
	t.Logf("Its fields are:")
	for _, fldname := range sch2.Fields() {
		var fieldType string
		sch2Type, err := sch2.Type(fldname)
		if err != nil {
			t.Error(err)
		}
		if sch2Type == record.SCHEMA_TYPE_INTEGER {
			fieldType = "int"
		} else {
			strlen, err := sch2.Length(fldname)
			if err != nil {
				t.Error(err)
			}
			fieldType = fmt.Sprintf("varchar(%d)", strlen)
		}
		t.Logf("%s: %s\n", fldname, fieldType)
	}

	// Part 2: Statistics Metadata
	ts, _ := record.NewTableScan(tx, "MyTable", layout)
	for i := 0; i < 50; i++ {
		ts.Insert()
		n := int(math.Round(rand.Float64() * 50))
		ts.SetInt("A", n)
		ts.SetString("B", fmt.Sprintf("rec%d", n))
	}
	si, _ := mdm.GetStatInfo("MyTable", layout, tx)
	t.Logf("B(MyTable) = %d\n", si.BlocksAccessed())
	t.Logf("R(MyTable) = %d\n", si.RecordsOutput())
	t.Logf("V(MyTable,A) = %d\n", si.DistinctValues("A"))
	t.Logf("V(MyTable,B) = %d\n", si.DistinctValues("B"))

	// Part 3: View Metadata
	// blocksizeが476使われており、400だとエラーになる
	viewdef := "select B from MyTable where A = 1"
	mdm.CreateView("viewA", viewdef, tx)
	v, _ := mdm.GetViewDef("viewA", tx)
	t.Logf("View def = %s\n", v)

	// t.Error()

	// TODO:
	// Part 4: Index Metadata
	// mdm.CreateIndex("indexA", "MyTable", "A", tx)
	// mdm.CreateIndex("indexB", "MyTable", "B", tx)
	// idxmap := mdm.GetIndexInfo("MyTable", tx)

	// ii := idxmap["A"]
	// t.Logf("B(indexA) = %d\n", ii.BlocksAccessed())
	// t.Logf("R(indexA) = %d\n", ii.RecordsOutput())
	// t.Logf("V(indexA,A) = %d\n", ii.DistinctValues("A"))
	// t.Logf("V(indexA,B) = %d\n", ii.DistinctValues("B"))

	// ii = idxmap["B"]
	// t.Logf("B(indexB) = %d\n", ii.BlocksAccessed())
	// t.Logf("R(indexB) = %d\n", ii.RecordsOutput())
	// t.Logf("V(indexB,A) = %d\n", ii.DistinctValues("A"))
	// t.Logf("V(indexB,B) = %d\n", ii.DistinctValues("B"))

	// tx.Commit()
}
