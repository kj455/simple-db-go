package transaction

import (
	"sync"
	"testing"
	"time"

	"github.com/kj455/simple-db/pkg/buffer"
	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/log"
	"github.com/kj455/simple-db/pkg/testutil"
	"github.com/kj455/simple-db/pkg/tx"
	"github.com/stretchr/testify/assert"
)

func TestTransaction(t *testing.T) {
	t.Parallel()
	const blockSize = 400
	dir, cleanup := testutil.SetupDir("test_transaction")
	t.Cleanup(cleanup)
	fileMgr := file.NewFileMgr(dir, blockSize)
	logMgr, err := log.NewLogMgr(fileMgr, "test_transaction_log")
	assert.NoError(t, err)
	const buffNum = 2
	buffs := make([]buffer.Buffer, buffNum)
	for i := 0; i < buffNum; i++ {
		buffs[i] = buffer.NewBuffer(fileMgr, logMgr, blockSize)
	}
	bm := buffer.NewBufferMgr(buffs)
	txNumGen := NewTxNumberGenerator()

	// TX1
	tx1, err := NewTransaction(fileMgr, logMgr, bm, txNumGen)
	assert.NoError(t, err)
	assert.Equal(t, buffNum, tx1.AvailableBuffs())

	block := file.NewBlockId("test_transaction", 0)
	tx1.Pin(block)
	tx1.SetInt(block, 80, 1, false)
	tx1.SetString(block, 40, "one", false)
	tx1.Commit()

	// TX2
	tx2, err := NewTransaction(fileMgr, logMgr, bm, txNumGen)
	assert.NoError(t, err)
	tx2.Pin(block)

	intVal, err := tx2.GetInt(block, 80)
	assert.NoError(t, err)
	assert.Equal(t, 1, intVal)

	strVal, err := tx2.GetString(block, 40)
	assert.NoError(t, err)
	assert.Equal(t, "one", strVal)
	tx2.Commit()

	// TX3
	tx3, err := NewTransaction(fileMgr, logMgr, bm, txNumGen)
	assert.NoError(t, err)
	tx3.Pin(block)
	tx3.SetInt(block, 80, 9999, true)
	intVal, err = tx3.GetInt(block, 80)
	assert.NoError(t, err)
	assert.Equal(t, 9999, intVal)
	tx3.Rollback()

	// TX4
	tx4, err := NewTransaction(fileMgr, logMgr, bm, txNumGen)
	assert.NoError(t, err)
	tx4.Pin(block)
	intVal, err = tx4.GetInt(block, 4)
	assert.NoError(t, err)
	assert.Equal(t, 0, intVal)
	tx4.Commit()
}

func TestTransaction_Concurrency(t *testing.T) {
	t.Parallel()
	const (
		blockSize    = 400
		dirname      = "test_transaction_concurrency"
		testFileName = "concurrency"
	)
	dir, cleanup := testutil.SetupDir("test_transaction_concurrency")
	t.Cleanup(cleanup)
	fm := file.NewFileMgr(dir, blockSize)
	lm, _ := log.NewLogMgr(fm, testFileName)
	buffs := make([]buffer.Buffer, 2)
	for i := 0; i < 2; i++ {
		buffs[i] = buffer.NewBuffer(fm, lm, blockSize)
	}
	bm := buffer.NewBufferMgr(buffs)
	txNumGen := NewTxNumberGenerator()
	blk1 := file.NewBlockId(testFileName, 1)
	blk2 := file.NewBlockId(testFileName, 2)

	wg := &sync.WaitGroup{}
	var A, B, C func(*testing.T, file.FileMgr, log.LogMgr, buffer.BufferMgr, tx.TxNumberGenerator)
	wg.Add(3)
	A = func(t *testing.T, fm file.FileMgr, lm log.LogMgr, bm buffer.BufferMgr, tng tx.TxNumberGenerator) {
		txA, _ := NewTransaction(fm, lm, bm, txNumGen)
		txA.Pin(blk1)
		txA.Pin(blk2)

		t.Log("Tx A: request slock 1")
		txA.GetInt(blk1, 0)

		t.Log("Tx A: receive slock 1")
		time.Sleep(1 * time.Second)

		t.Log("Tx A: request slock 2")
		txA.GetInt(blk2, 0)

		t.Log("Tx A: receive slock 2")
		txA.Commit()

		t.Log("Tx A: commit")
		wg.Done()
	}
	B = func(t *testing.T, fm file.FileMgr, lm log.LogMgr, bm buffer.BufferMgr, txNumGen tx.TxNumberGenerator) {
		txB, _ := NewTransaction(fm, lm, bm, txNumGen)
		txB.Pin(blk1)
		txB.Pin(blk2)

		t.Log("Tx B: request xlock 2")
		txB.SetInt(blk2, 0, 200, false)

		t.Log("Tx B: receive xlock 2")
		time.Sleep(1 * time.Second)

		t.Log("Tx B: request slock 1")
		txB.GetInt(blk1, 0)

		t.Log("Tx B: receive slock 1")
		txB.Commit()

		t.Log("Tx B: commit")
		wg.Done()
	}
	C = func(t *testing.T, fm file.FileMgr, lm log.LogMgr, bm buffer.BufferMgr, txNumGen tx.TxNumberGenerator) {
		txC, _ := NewTransaction(fm, lm, bm, txNumGen)
		txC.Pin(blk1)
		txC.Pin(blk2)
		time.Sleep(500 * time.Millisecond)

		t.Log("Tx C: request xlock 1")
		txC.SetInt(blk1, 0, 100, false)

		t.Log("Tx C: receive xlock 1")
		time.Sleep(1 * time.Second)

		t.Log("Tx C: request slock 2")
		txC.GetInt(blk2, 0)

		t.Log("Tx C: receive slock 2")
		txC.Commit()

		t.Log("Tx C: commit")
		wg.Done()
	}

	go A(t, fm, lm, bm, txNumGen)
	go B(t, fm, lm, bm, txNumGen)
	go C(t, fm, lm, bm, txNumGen)

	wg.Wait()

	buffs[0].AssignToBlock(blk1)
	buffs[1].AssignToBlock(blk2)
	assert.Equal(t, uint32(100), buffs[0].Contents().GetInt(0))
	assert.Equal(t, uint32(200), buffs[1].Contents().GetInt(0))
}

func TestTransaction_Size(t *testing.T) {
	t.Parallel()
	const (
		blockSize   = 400
		dirname     = "test_transaction_size"
		fileName    = "file"
		logFileName = "log"
	)
	dir, cleanup := testutil.SetupDir(dirname)
	t.Cleanup(cleanup)
	fileMgr := file.NewFileMgr(dir, blockSize)
	logMgr, err := log.NewLogMgr(fileMgr, logFileName)
	assert.NoError(t, err)
	buf := buffer.NewBuffer(fileMgr, logMgr, blockSize)
	bm := buffer.NewBufferMgr([]buffer.Buffer{buf})
	txNumGen := NewTxNumberGenerator()
	tx, err := NewTransaction(fileMgr, logMgr, bm, txNumGen)
	assert.NoError(t, err)

	size, err := tx.Size(fileName)

	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	block := file.NewBlockId(fileName, 0)
	buf.AssignToBlock(block)
	buf.WriteContents(1, 1, func(p buffer.ReadWritePage) {
		p.SetInt(0, 0)
	})
	bm.FlushAll(1)

	size, err = tx.Size(fileName)

	assert.NoError(t, err)
	assert.Equal(t, 1, size)
}

func TestTransaction_Append(t *testing.T) {
	t.Parallel()
	const (
		blockSize   = 400
		fileName    = "test_transaction_append"
		logFileName = "test_transaction_append_log"
	)
	dir, _, cleanup := testutil.SetupFile(fileName)
	t.Cleanup(cleanup)
	_, _, cleanupLog := testutil.SetupFile(logFileName)
	defer cleanupLog()
	fileMgr := file.NewFileMgr(dir, blockSize)
	logMgr, err := log.NewLogMgr(fileMgr, logFileName)
	assert.NoError(t, err)
	buf := buffer.NewBuffer(fileMgr, logMgr, blockSize)
	bm := buffer.NewBufferMgr([]buffer.Buffer{buf})
	txNumGen := NewTxNumberGenerator()
	tx, err := NewTransaction(fileMgr, logMgr, bm, txNumGen)
	assert.NoError(t, err)

	block, err := tx.Append(fileName)

	assert.NoError(t, err)
	assert.True(t, block.Equals(file.NewBlockId(fileName, 0)))
}
