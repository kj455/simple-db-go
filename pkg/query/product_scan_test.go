package query

import (
	"testing"

	"github.com/kj455/simple-db/pkg/buffer"
	buffermgr "github.com/kj455/simple-db/pkg/buffer_mgr"
	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/log"
	"github.com/kj455/simple-db/pkg/metadata"
	"github.com/kj455/simple-db/pkg/record"
	"github.com/kj455/simple-db/pkg/testutil"
	"github.com/kj455/simple-db/pkg/tx/transaction"
	"github.com/stretchr/testify/assert"
)

func TestProductScan(t *testing.T) {
	const (
		blockSize    = 400
		testFileName = "test_product_scan"
		tableNameA   = "table_test_product_scan_A"
		tableNameB   = "table_test_product_scan_B"
	)
	dir, _, cleanup := testutil.SetupFile(testFileName)
	_, _, cleanupTableA := testutil.SetupFile(tableNameA)
	_, _, cleanupTableB := testutil.SetupFile(tableNameB)
	defer func() {
		cleanup()
		cleanupTableA()
		cleanupTableB()
	}()
	fm := file.NewFileMgr(dir, blockSize)
	lm, _ := log.NewLogMgr(fm, testFileName)
	buffs := make([]buffer.Buffer, 10)
	for i := range buffs {
		buffs[i] = buffer.NewBuffer(fm, lm, blockSize)
	}
	bm := buffermgr.NewBufferMgr(buffs)
	txNumGen := transaction.NewTxNumberGenerator()
	tx, err := transaction.NewTransaction(fm, lm, bm, txNumGen)
	assert.NoError(t, err)
	_, err = metadata.NewMetadataMgr(tx)
	assert.NoError(t, err)

	// Create a table scan1
	sch1 := record.NewSchema()
	sch1.AddIntField("A")
	sch1.AddStringField("B", 10)
	layout, _ := record.NewLayoutFromSchema(sch1)
	ts1, err := record.NewTableScan(tx, tableNameA, layout)
	assert.NoError(t, err)
	defer ts1.Close()
	ts1.BeforeFirst()

	// Create a table scan2
	sch2 := record.NewSchema()
	sch2.AddIntField("C")
	sch2.AddStringField("D", 10)
	layout2, err := record.NewLayoutFromSchema(sch2)
	assert.NoError(t, err)
	ts2, err := record.NewTableScan(tx, tableNameB, layout2)
	assert.NoError(t, err)
	defer ts2.Close()

	// Insert records
	err = ts1.Insert()
	assert.NoError(t, err)
	ts1.SetInt("A", 100)
	ts1.SetString("B", "recordB1")
	err = ts1.Insert()
	assert.NoError(t, err)
	ts1.SetInt("A", 101)
	ts1.SetString("B", "recordB2")

	// Insert records
	err = ts2.Insert()
	assert.NoError(t, err)
	ts2.SetInt("C", 200)
	ts2.SetString("D", "recordD1")
	err = ts2.Insert()
	assert.NoError(t, err)
	ts2.SetInt("C", 201)
	ts2.SetString("D", "recordD2")

	// Create a product scan
	prodScan, err := NewProductScan(ts1, ts2)
	assert.NoError(t, err)

	// Test the product scan 1st record
	prodScan.BeforeFirst()

	assert.True(t, prodScan.Next())

	valA, err := prodScan.GetInt("A")
	assert.NoError(t, err)
	assert.Equal(t, 100, valA)

	valB, err := prodScan.GetString("B")
	assert.NoError(t, err)
	assert.Equal(t, "recordB1", valB)

	valC, err := prodScan.GetInt("C")
	assert.NoError(t, err)
	assert.Equal(t, 200, valC)

	valD, err := prodScan.GetString("D")
	assert.NoError(t, err)
	assert.Equal(t, "recordD1", valD)

	// Test the product scan 2nd record
	assert.True(t, prodScan.Next())

	valA, err = prodScan.GetInt("A")
	assert.NoError(t, err)
	assert.Equal(t, 100, valA)

	valB, err = prodScan.GetString("B")
	assert.NoError(t, err)
	assert.Equal(t, "recordB1", valB)

	valC, err = prodScan.GetInt("C")
	assert.NoError(t, err)
	assert.Equal(t, 201, valC)

	valD, err = prodScan.GetString("D")
	assert.NoError(t, err)
	assert.Equal(t, "recordD2", valD)

	// Test the product scan 3rd record
	assert.True(t, prodScan.Next())

	valA, err = prodScan.GetInt("A")
	assert.NoError(t, err)
	assert.Equal(t, 101, valA)

	valB, err = prodScan.GetString("B")
	assert.NoError(t, err)
	assert.Equal(t, "recordB2", valB)

	valC, err = prodScan.GetInt("C")
	assert.NoError(t, err)
	assert.Equal(t, 200, valC)

	valD, err = prodScan.GetString("D")
	assert.NoError(t, err)
	assert.Equal(t, "recordD1", valD)

	// Test the product scan 4th record
	assert.True(t, prodScan.Next())

	valA, err = prodScan.GetInt("A")
	assert.NoError(t, err)
	assert.Equal(t, 101, valA)

	valB, err = prodScan.GetString("B")
	assert.NoError(t, err)
	assert.Equal(t, "recordB2", valB)

	valC, err = prodScan.GetInt("C")
	assert.NoError(t, err)
	assert.Equal(t, 201, valC)

	valD, err = prodScan.GetString("D")
	assert.NoError(t, err)
	assert.Equal(t, "recordD2", valD)

}
