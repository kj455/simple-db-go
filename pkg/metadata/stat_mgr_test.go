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

func TestStatMgr(t *testing.T) {
	t.Skip("skipping test")
	const (
		logFileName = "test_stat_mgr_log"
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
	bufferMgr := buffermgr.NewBufferMgr(buffs, buffermgr.WithMaxWaitTime(0))
	txNumGen := transaction.NewTxNumberGenerator()
	tx, err := transaction.NewTransaction(fileMgr, logMgr, bufferMgr, txNumGen)
	assert.NoError(t, err)
	tblMgr, err := NewTableMgr(tx)
	assert.NoError(t, err)

	statMgr, err := NewStatMgr(tblMgr, tx)

	assert.NoError(t, err)

	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 10)
	layout, err := record.NewLayoutFromSchema(schema)
	assert.NoError(t, err)

	const tableName = "test_stat_mgr_table"
	stat, err := statMgr.GetStatInfo(tableName, layout, tx)
	defer func() {
		err := tblMgr.DropTable(tableName, tx)
		assert.NoError(t, err)
	}()
	assert.NoError(t, err)
	assert.Equal(t, 0, stat.BlocksAccessed())
	assert.Equal(t, 0, stat.RecordsOutput())
}
