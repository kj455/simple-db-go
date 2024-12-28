package query

import (
	"testing"

	"github.com/kj455/simple-db/pkg/buffer"
	buffermgr "github.com/kj455/simple-db/pkg/buffer_mgr"
	"github.com/kj455/simple-db/pkg/constant"
	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/log"
	"github.com/kj455/simple-db/pkg/metadata"
	"github.com/kj455/simple-db/pkg/record"
	"github.com/kj455/simple-db/pkg/testutil"
	"github.com/kj455/simple-db/pkg/tx/transaction"
	"github.com/stretchr/testify/assert"
)

func TestSelectScan(t *testing.T) {
	const (
		blockSize    = 400
		testFileName = "test_select_scan"
		tableName    = "test_select_scan"
	)
	dir, _, cleanup := testutil.SetupFile(testFileName)
	t.Cleanup(cleanup)
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

	// Create a predicate
	constA, err := constant.NewConstant(constant.KIND_INT, 100)
	assert.NoError(t, err)
	termA := NewTerm(NewFieldExpression("A"), NewConstantExpression(constA))

	constB, err := constant.NewConstant(constant.KIND_STR, "record")
	assert.NoError(t, err)
	termB := NewTerm(NewFieldExpression("B"), NewConstantExpression(constB))

	pred := NewPredicate(termA, termB)

	// Create a SelectScan
	selectScan := NewSelectScan(tableScan, pred)
	defer selectScan.Close()

	err = selectScan.BeforeFirst()
	assert.NoError(t, err)

	// Check the record
	assert.True(t, selectScan.Next())
	valA, err := selectScan.GetInt("A")
	assert.NoError(t, err)
	assert.Equal(t, 100, valA)

	valB, err := selectScan.GetString("B")
	assert.NoError(t, err)
	assert.Equal(t, "record", valB)

	tx.Commit()
}
