package transaction

import (
	"fmt"

	"github.com/kj455/db/pkg/buffer"
	buffermgr "github.com/kj455/db/pkg/buffer_mgr"
	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/tx"
)

type TransactionImpl struct {
	recoveryMgr tx.RecoveryMgr
	concurMgr   tx.ConcurrencyMgr
	buffs       tx.BufferList
	bm          buffermgr.BufferMgr
	fm          file.FileMgr
	txNum       int
}

const END_OF_FILE = -1

func NewTransaction(fm file.FileMgr, lm log.LogMgr, bm buffermgr.BufferMgr, txNumGen tx.TxNumberGenerator) (*TransactionImpl, error) {
	txNum := txNumGen.Next()
	cm := NewConcurrencyMgr()
	rm, err := NewRecoveryMgr(nil, txNum, lm, bm)
	if err != nil {
		return nil, fmt.Errorf("tx: failed to create recovery manager: %w", err)
	}
	tx := &TransactionImpl{
		fm:          fm,
		bm:          bm,
		recoveryMgr: rm,
		concurMgr:   cm,
		txNum:       txNum,
		buffs:       NewBufferList(bm),
	}
	rm.tx = tx
	return tx, nil
}

func (t *TransactionImpl) Commit() error {
	if err := t.recoveryMgr.Commit(); err != nil {
		return fmt.Errorf("tx: failed to commit: %w", err)
	}
	t.concurMgr.Release()
	t.buffs.UnpinAll()
	return nil
}

func (t *TransactionImpl) Rollback() error {
	if err := t.recoveryMgr.Rollback(); err != nil {
		return fmt.Errorf("tx: failed to rollback: %w", err)
	}
	t.concurMgr.Release()
	t.buffs.UnpinAll()
	return nil
}

func (t *TransactionImpl) Recover() error {
	if err := t.bm.FlushAll(t.txNum); err != nil {
		return fmt.Errorf("tx: failed to flush all: %w", err)
	}
	if err := t.recoveryMgr.Recover(); err != nil {
		return fmt.Errorf("tx: failed to recover: %w", err)
	}
	return nil
}

func (t *TransactionImpl) Pin(block file.BlockId) error {
	return t.buffs.Pin(block)
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
	buff.WriteContents(t.txNum, lsn, func(p buffer.ReadWritePage) {
		p.SetInt(offset, uint32(val))
	})
	return nil
}

func (t *TransactionImpl) SetString(block file.BlockId, offset int, val string, okToLog bool) error {
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
		lsn, err = t.recoveryMgr.SetString(buff, offset, val)
		if err != nil {
			return fmt.Errorf("tx: failed to set string: %w", err)
		}
	}
	buff.WriteContents(t.txNum, lsn, func(p buffer.ReadWritePage) {
		p.SetString(offset, val)
	})
	return nil
}

// Size returns the number of blocks in the specified file.
func (t *TransactionImpl) Size(filename string) (int, error) {
	dummy := file.NewBlockId(filename, END_OF_FILE)
	if err := t.concurMgr.SLock(dummy); err != nil {
		return 0, fmt.Errorf("tx: failed to SLock dummy block: %w", err)
	}
	len, err := t.fm.BlockNum(filename)
	if err != nil {
		return 0, fmt.Errorf("tx: failed to get size: %w", err)
	}
	return len, nil
}

func (t *TransactionImpl) Append(filename string) (file.BlockId, error) {
	dummy := file.NewBlockId(filename, END_OF_FILE)
	if err := t.concurMgr.XLock(dummy); err != nil {
		return nil, fmt.Errorf("tx: failed to XLock dummy block: %w", err)
	}
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
