package metadata

import (
	"fmt"
	"testing"

	"github.com/kj455/db/pkg/buffer"
	buffermgr "github.com/kj455/db/pkg/buffer_mgr"
	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/record"
	"github.com/kj455/db/pkg/testutil"
	"github.com/kj455/db/pkg/tx/transaction"
)

func TestViewMgr(t *testing.T) {
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
	tm, _ := NewTableMgr(true, tx)

	sch := record.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	tm.CreateTable("MyTable", sch, tx)

	layout, _ := tm.GetLayout("MyTable", tx)
	size := layout.SlotSize()
	sch2 := layout.Schema()

	t.Logf("MyTable has slot size %d\n", size)
	t.Logf("Its fields are:\n")

	for _, fldname := range sch2.Fields() {
		var typeStr string
		sch2Type, err := sch2.Type(fldname)
		if err != nil {
			t.Error(err)
		}
		if sch2Type == record.SCHEMA_TYPE_INTEGER {
			typeStr = "int"
		} else {
			strlen, err := sch2.Length(fldname)
			if err != nil {
				t.Error(err)
			}
			typeStr = fmt.Sprintf("varchar(%d)", strlen)
		}
		t.Logf("%s: %s\n", fldname, typeStr)
	}
	tx.Commit()
}
