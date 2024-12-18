package query

import (
	"testing"

	"github.com/kj455/db/pkg/buffer"
	buffermgr "github.com/kj455/db/pkg/buffer_mgr"
	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/metadata"
	"github.com/kj455/db/pkg/record"
	"github.com/kj455/db/pkg/testutil"
	"github.com/kj455/db/pkg/tx/transaction"
	"github.com/stretchr/testify/assert"
)

func TestProjectScan(t *testing.T) {
	const (
		blockSize    = 400
		testFileName = "test_project_scan"
		tableName    = "table_test_project_scan"
	)
	dir, _, cleanup := testutil.SetupFile(testFileName)
	defer cleanup()
	fm := file.NewFileMgr(dir, blockSize)
	lm, _ := log.NewLogMgr(fm, testFileName)
	buffs := make([]buffer.Buffer, 2)
	for i := range buffs {
		buffs[i] = buffer.NewBuffer(fm, lm, blockSize)
	}
	bm := buffermgr.NewBufferMgr(buffs)
	txNumGen := transaction.NewTxNumberGenerator()
	tx, err := transaction.NewTransaction(fm, lm, bm, txNumGen)
	assert.NoError(t, err)
	_, err = metadata.NewMetadataMgr(tx)
	assert.NoError(t, err)

	sch := record.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 10)

	layout, _ := record.NewLayoutFromSchema(sch)
	tableScan, err := record.NewTableScan(tx, tableName, layout)
	assert.NoError(t, err)
	defer tableScan.Close()
	tableScan.BeforeFirst()

	// Insert a record
	err = tableScan.Insert()
	assert.NoError(t, err)
	tableScan.SetInt("A", 100)
	tableScan.SetString("B", "record")

	// Create a project scan
	projectScan := NewProjectScan(tableScan, []string{"A"})

	// Check if the project scan has the specified field
	assert.Equal(t, true, projectScan.HasField("A"))
	assert.Equal(t, false, projectScan.HasField("B"))

	projectScan.BeforeFirst()

	// Check if the project scan has the specified field value
	assert.Equal(t, true, projectScan.Next())
	val, err := projectScan.GetInt("A")
	assert.NoError(t, err)
	assert.Equal(t, 100, val)
	_, err = projectScan.GetInt("B")
	assert.Error(t, err)

	tx.Commit()
}
