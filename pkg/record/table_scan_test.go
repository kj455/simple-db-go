package record

import (
	"fmt"
	"testing"

	"github.com/kj455/simple-db/pkg/buffer"
	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/log"
	"github.com/kj455/simple-db/pkg/testutil"
	"github.com/kj455/simple-db/pkg/tx/transaction"
	"github.com/stretchr/testify/assert"
)

func TestTableScan(t *testing.T) {
	t.Parallel()
	const (
		blockSize    = 800
		testFileName = "file"
	)
	dir, cleanup := testutil.SetupDir("test_table_scan")
	t.Cleanup(cleanup)

	fm := file.NewFileMgr(dir, blockSize)
	lm, err := log.NewLogMgr(fm, testFileName)
	assert.NoError(t, err)
	buff := buffer.NewBuffer(fm, lm, blockSize)
	buff2 := buffer.NewBuffer(fm, lm, blockSize)
	bm := buffer.NewBufferMgr([]buffer.Buffer{buff, buff2})
	txNumGen := transaction.NewTxNumberGenerator()
	tx, err := transaction.NewTransaction(fm, lm, bm, txNumGen)
	assert.NoError(t, err)

	sch := NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 4)

	layout, err := NewLayoutFromSchema(sch)
	assert.NoError(t, err)

	block, err := tx.Append(testFileName)
	assert.NoError(t, err)
	tx.Pin(block)
	_, err = NewRecordPage(tx, block, layout)
	assert.NoError(t, err)

	scan, err := NewTableScan(tx, testFileName, layout)
	assert.NoError(t, err)
	scan.BeforeFirst()

	// Insert 10 records
	for i := 0; i < 10; i++ {
		err = scan.Insert()
		assert.NoError(t, err)
		err = scan.SetInt("A", i)
		assert.NoError(t, err)
		err = scan.SetString("B", fmt.Sprintf("rec%d", i))
		assert.NoError(t, err)
	}

	// Scan the records
	rid := NewRID(0, -1)
	scan.MoveToRID(rid)
	count := 0
	for scan.Next() {
		a, err := scan.GetInt("A")
		assert.NoError(t, err)
		b, err := scan.GetString("B")
		assert.NoError(t, err)
		assert.Equal(t, count, a)
		assert.Equal(t, fmt.Sprintf("rec%d", count), b)
		count++
	}
	assert.Equal(t, 10, count)
}
