package metadata

import (
	"testing"

	"github.com/kj455/simple-db/pkg/buffer"
	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/log"
	"github.com/kj455/simple-db/pkg/testutil"
	"github.com/kj455/simple-db/pkg/tx/transaction"
	"github.com/stretchr/testify/assert"
)

func TestViewMgr(t *testing.T) {
	t.Skip("skipping test")
	const (
		logFileName = "test_view_mgr_log"
		blockSize   = 1024
	)
	dir, _, cleanup := testutil.SetupFile(logFileName)
	t.Cleanup(cleanup)
	fileMgr := file.NewFileMgr(dir, blockSize)
	logMgr, err := log.NewLogMgr(fileMgr, logFileName)
	assert.NoError(t, err)
	buffNum := 10
	buffs := make([]buffer.Buffer, buffNum)
	for i := range buffs {
		buffs[i] = buffer.NewBuffer(fileMgr, logMgr, blockSize)
	}
	bufferMgr := buffer.NewBufferMgr(buffs, buffer.WithMaxWaitTime(0))
	txNumGen := transaction.NewTxNumberGenerator()
	tx, err := transaction.NewTransaction(fileMgr, logMgr, bufferMgr, txNumGen)
	assert.NoError(t, err)
	tblMgr, err := NewTableMgr(tx)
	assert.NoError(t, err)

	viewMgr, err := NewViewMgr(tblMgr, tx)
	assert.NoError(t, err)
	defer func() {
		err := tblMgr.DropTable(tableViewCatalog, tx)
		assert.NoError(t, err)
	}()

	const (
		viewName = "test_view"
		viewDef  = "SELECT A, B FROM test_table"
	)
	err = viewMgr.CreateView(viewName, viewDef, tx)
	assert.NoError(t, err)
	defer func() {
		err := viewMgr.DeleteView(viewName, tx)
		assert.NoError(t, err)
	}()
	def, err := viewMgr.GetViewDef(viewName, tx)
	assert.NoError(t, err)
	assert.Equal(t, viewDef, def)
}
