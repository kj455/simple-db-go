package metadata

import (
	"testing"

	"github.com/kj455/simple-db/pkg/buffer"
	buffermgr "github.com/kj455/simple-db/pkg/buffer_mgr"
	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/log"
	"github.com/kj455/simple-db/pkg/record"
	"github.com/kj455/simple-db/pkg/testutil"
	"github.com/kj455/simple-db/pkg/tx/transaction"
	"github.com/stretchr/testify/assert"
)

func TestTableMgr(t *testing.T) {
	const (
		logFileName = "test_table_mgr_log"
		blockSize   = 400
		tableName   = "test_table_mgr_table"
	)
	dir, _, cleanup := testutil.SetupFile(logFileName)
	t.Cleanup(cleanup)
	fileMgr := file.NewFileMgr(dir, blockSize)
	logMgr, err := log.NewLogMgr(fileMgr, logFileName)
	assert.NoError(t, err)
	buff1 := buffer.NewBuffer(fileMgr, logMgr, blockSize)
	buff2 := buffer.NewBuffer(fileMgr, logMgr, blockSize)
	buff3 := buffer.NewBuffer(fileMgr, logMgr, blockSize)
	bufferMgr := buffermgr.NewBufferMgr([]buffer.Buffer{buff1, buff2, buff3})
	txNumGen := transaction.NewTxNumberGenerator()
	tx, err := transaction.NewTransaction(fileMgr, logMgr, bufferMgr, txNumGen)
	assert.NoError(t, err)

	tblMgr, err := NewTableMgr(tx)
	assert.NoError(t, err)
	defer func() {
		err := tblMgr.DropTable(tblMgr.tableCatalog, tx)
		assert.NoError(t, err)
		err = tblMgr.DropTable(tblMgr.fieldCatalog, tx)
		assert.NoError(t, err)
	}()

	// Create a table
	sch := record.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 10)
	err = tblMgr.CreateTable(tableName, sch, tx)
	// Drop the table
	defer func() {
		err = tblMgr.DropTable(tableName, tx)
		assert.NoError(t, err)
	}()
	assert.NoError(t, err)

	// Check the table's layout
	layout, err := tblMgr.GetLayout(tableName, tx)
	assert.NoError(t, err)

	fields := layout.Schema().Fields()
	assert.Equal(t, 2, len(fields))
	assert.Equal(t, "A", fields[0])
	assert.Equal(t, "B", fields[1])
	l, err := layout.Schema().Length("B")
	assert.NoError(t, err)
	assert.Equal(t, 10, l)

	tx.Commit()
}
