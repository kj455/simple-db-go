package transaction

import (
	"fmt"
	"sync"
	"sync/atomic"

	buffermgr "github.com/kj455/db/pkg/buffer_mgr"
	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/tx"
)

type TransactionImpl struct {
	recoveryMgr tx.RecoveryMgr
	concurMgr   tx.ConcurrencyMgr
	bm          buffermgr.BufferMgr
	buffs       tx.BufferList
	fm          file.FileMgr
	txNum       int
}

var nextTxNum int32 = 0

const END_OF_FILE = -1

func NewTransaction(fm file.FileMgr, lm log.LogMgr, bm buffermgr.BufferMgr) (*TransactionImpl, error) {
	txnum := nextTxNumber()
	cm := NewConcurrencyMgr()
	rm, err := NewRecoveryMgr(nil, txnum, lm, bm)
	if err != nil {
		return nil, err
	}
	tx := &TransactionImpl{
		fm:          fm,
		bm:          bm,
		txNum:       txnum,
		recoveryMgr: rm,
		concurMgr:   cm,
		buffs:       NewBufferList(bm),
	}
	// rm.tx = tx
	return tx, nil
}

func (t *TransactionImpl) Commit() {
	t.recoveryMgr.Commit()
	// fmt.Println("transaction", t.txnum, "committed")
	t.concurMgr.Release()
	t.buffs.UnpinAll()
}

func (t *TransactionImpl) Rollback() {
	t.recoveryMgr.Rollback()
	// fmt.Println("transaction", t.txnum, "rolled back")
	t.concurMgr.Release()
	t.buffs.UnpinAll()
}

func (t *TransactionImpl) Recover() {
	t.bm.FlushAll(t.txNum)
	t.recoveryMgr.Recover()
}

func (t *TransactionImpl) Pin(block file.BlockId) {
	t.buffs.Pin(block)
}

func (t *TransactionImpl) Unpin(block file.BlockId) {
	t.buffs.Unpin(block)
}

func (t *TransactionImpl) GetInt(block file.BlockId, offset int) (int, error) {
	if err := t.concurMgr.SLock(block); err != nil {
		return 0, fmt.Errorf("tx: failed to get int: %w", err)
	}
	buff, ok := t.buffs.GetBuffer(block)
	if !ok {
		return 0, fmt.Errorf("tx: buffer not found for block %v", block)
	}
	val := buff.Contents().GetInt(offset)
	return int(val), nil
}

func (t *TransactionImpl) GetString(block file.BlockId, offset int) (string, error) {
	if err := t.concurMgr.SLock(block); err != nil {
		return "", fmt.Errorf("tx: failed to get string: %w", err)
	}
	buff, ok := t.buffs.GetBuffer(block)
	if !ok {
		return "", fmt.Errorf("tx: buffer not found for block %v", block)
	}
	val := buff.Contents().GetString(offset)
	return val, nil
}

func (t *TransactionImpl) SetInt(block file.BlockId, offset int, val int, okToLog bool) error {
	if err := t.concurMgr.XLock(block); err != nil {
		return fmt.Errorf("tx: failed to XLock block %v: %w", block, err)
	}
	buff, ok := t.buffs.GetBuffer(block)
	if !ok {
		return fmt.Errorf("tx: buffer not found for block %v", block)
	}
	var lsn int = -1
	if okToLog {
		var err error
		lsn, err = t.recoveryMgr.SetInt(buff, offset, val)
		if err != nil {
			return fmt.Errorf("tx: failed to set int: %w", err)
		}
	}
	buff.WriteContents(t.txNum, lsn, func(p file.ReadWritePage) {
		p.SetInt(offset, uint32(val))
	})
	return nil
}

func (t *TransactionImpl) SetString(block file.BlockId, offset int, val string, okToLog bool) error {
	t.concurMgr.XLock(block)
	buff, ok := t.buffs.GetBuffer(block)
	if !ok {
		return fmt.Errorf("tx: buffer not found for block %v", block)
	}
	var lsn int = -1
	if okToLog {
		var err error
		lsn, err = t.recoveryMgr.SetString(buff, offset, val)
		if err != nil {
			return fmt.Errorf("tx: failed to set string: %w", err)
		}
	}
	buff.WriteContents(t.txNum, lsn, func(p file.ReadWritePage) {
		p.SetString(offset, val)
	})
	return nil
}

func (t *TransactionImpl) Size(filename string) (int, error) {
	dummy := file.NewBlockId(filename, END_OF_FILE)
	t.concurMgr.SLock(dummy)
	len, err := t.fm.Length(filename)
	if err != nil {
		return 0, fmt.Errorf("tx: failed to get size: %w", err)
	}
	return len, nil
}

func (t *TransactionImpl) Append(filename string) (file.BlockId, error) {
	dummy := file.NewBlockId(filename, END_OF_FILE)
	t.concurMgr.XLock(dummy)
	block, err := t.fm.Append(filename)
	if err != nil {
		return nil, fmt.Errorf("tx: failed to append: %w", err)
	}
	return block, nil
}

func (t *TransactionImpl) BlockSize() int {
	return t.fm.BlockSize()
}

func (t *TransactionImpl) AvailableBuffs() int {
	return t.bm.AvailableNum()
}

var mu sync.Mutex

func nextTxNumber() int {
	mu.Lock()
	defer mu.Unlock()
	num := atomic.AddInt32(&nextTxNum, 1)
	return int(num)
}
