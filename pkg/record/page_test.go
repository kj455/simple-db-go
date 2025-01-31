package record

import (
	"testing"

	"github.com/kj455/simple-db/pkg/buffer"
	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/log"
	"github.com/kj455/simple-db/pkg/testutil"
	transaction "github.com/kj455/simple-db/pkg/tx"
	"github.com/stretchr/testify/assert"
)

func TestRecordPage(t *testing.T) {
	t.Parallel()
	const (
		blockSize    = 400
		testFileName = "file"
	)
	dir, cleanup := testutil.SetupDir("test_record_page")
	t.Cleanup(cleanup)

	fm := file.NewFileMgr(dir, blockSize)
	lm, err := log.NewLogMgr(fm, testFileName)
	assert.NoError(t, err)
	buff := buffer.NewBuffer(fm, lm, blockSize)
	bm := buffer.NewBufferMgr([]buffer.Buffer{buff})
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
	recPage, err := NewRecordPage(tx, block, layout)
	assert.NoError(t, err)

	// Insert into Slot 0
	slot, err := recPage.InsertAfter(SLOT_INIT)
	assert.NoError(t, err)
	assert.Equal(t, 0, slot)
	err = recPage.SetInt(slot, "A", 1)
	assert.NoError(t, err)
	err = recPage.SetString(slot, "B", "rec1")
	assert.NoError(t, err)

	// Insert into Slot 1
	slot, err = recPage.InsertAfter(slot)
	assert.NoError(t, err)
	assert.Equal(t, 1, slot)
	err = recPage.SetInt(slot, "A", 2)
	assert.NoError(t, err)
	err = recPage.SetString(slot, "B", "rec2")
	assert.NoError(t, err)

	// Delete Slot 0
	err = recPage.Delete(0)
	assert.NoError(t, err)

	// Next Slot should be 1
	slot = recPage.NextAfter(SLOT_INIT)
	assert.Equal(t, 1, slot)

	// Format the page
	err = recPage.Format()
	assert.NoError(t, err)

	// Next Slot should be SLOT_INIT
	slot = recPage.NextAfter(SLOT_INIT)
	assert.Equal(t, SLOT_INIT, slot)
}
