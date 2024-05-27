package recovery

import (
	"fmt"

	"github.com/kj455/db/pkg/buffer"
	buffermgr "github.com/kj455/db/pkg/buffer_mgr"
	"github.com/kj455/db/pkg/log"
	"github.com/kj455/db/pkg/record"
	"github.com/kj455/db/pkg/tx"
)

// RecoveryMgr is an interface for recovery manager - undo only
type RecoveryMgr interface {
	Commit() error
	Rollback() error
	Recover() error
	SetInt(buff buffer.Buffer, offset int, val int) (int, error)
	SetString(buff buffer.Buffer, offset int, val string) (int, error)
}

type RecoveryMgrImpl struct {
	lm    log.LogMgr
	bm    buffermgr.BufferMgr
	tx    tx.Transaction
	txNum int
}

func NewRecoveryMgr(tx tx.Transaction, txNum int, lm log.LogMgr, bm buffermgr.BufferMgr) (*RecoveryMgrImpl, error) {
	rm := &RecoveryMgrImpl{
		lm:    lm,
		bm:    bm,
		tx:    tx,
		txNum: txNum,
	}
	_, err := record.WriteStartRecordToLog(lm, txNum)
	if err != nil {
		return nil, fmt.Errorf("recovery: failed to write start record to log: %v", err)
	}
	return rm, nil
}

// Commit writes a commit record to the log and flushes the buffer
func (rm *RecoveryMgrImpl) Commit() error {
	err := rm.bm.FlushAll(rm.txNum)
	if err != nil {
		return fmt.Errorf("recovery: failed to flush buffer: %v", err)
	}
	lsn, err := record.WriteCommitRecordToLog(rm.lm, rm.txNum)
	if err != nil {
		return fmt.Errorf("recovery: failed to write commit record to log: %v", err)
	}
	rm.lm.Flush(lsn)
	return nil
}

// Rollback rolls back the transaction by undoing log records and flushing the buffer
func (rm *RecoveryMgrImpl) Rollback() error {
	if err := rm.rollback(); err != nil {
		return fmt.Errorf("recovery: failed to rollback: %v", err)
	}
	if err := rm.bm.FlushAll(rm.txNum); err != nil {
		return fmt.Errorf("recovery: failed to flush buffer: %v", err)
	}
	lsn, err := record.WriteRollbackRecordToLog(rm.lm, rm.txNum)
	if err != nil {
		return fmt.Errorf("recovery: failed to write rollback record to log: %v", err)
	}
	err = rm.lm.Flush(lsn)
	if err != nil {
		return fmt.Errorf("recovery: failed to flush log: %v", err)
	}
	return nil
}

// Recover recovers modifications made by uncommitted transactions
func (rm *RecoveryMgrImpl) Recover() error {
	if err := rm.recover(); err != nil {
		return fmt.Errorf("recovery: failed to recover: %v", err)
	}
	if err := rm.bm.FlushAll(rm.txNum); err != nil {
		return fmt.Errorf("recovery: failed to flush buffer: %v", err)
	}
	lsn, err := record.WriteCommitRecordToLog(rm.lm, rm.txNum)
	if err != nil {
		return fmt.Errorf("recovery: failed to write commit record to log: %v", err)
	}
	rm.lm.Flush(lsn)
	return nil
}

func (rm *RecoveryMgrImpl) SetInt(buff buffer.Buffer, offset int, val int) (int, error) {
	return record.WriteSetIntRecordToLog(rm.lm, rm.txNum, buff.Block(), offset, val)
}

func (rm *RecoveryMgrImpl) SetString(buff buffer.Buffer, offset int, val string) (int, error) {
	return record.WriteSetStringRecordToLog(rm.lm, rm.txNum, buff.Block(), offset, val)
}

func (rm *RecoveryMgrImpl) rollback() error {
	iter, err := rm.lm.Iterator()
	if err != nil {
		return err
	}
	for iter.HasNext() {
		bytes, err := iter.Next()
		if err != nil {
			return err
		}
		rec := record.NewLogRecord(bytes)
		if rec.TxNum() != rm.txNum {
			continue
		}
		if rec.Op() == record.START {
			return nil
		}
		rec.Undo(rm.tx)
	}
	return nil
}

func (rm *RecoveryMgrImpl) recover() error {
	finishedTxs := make(map[int]bool)
	iter, err := rm.lm.Iterator()
	if err != nil {
		return err
	}
	for iter.HasNext() {
		bytes, err := iter.Next()
		if err != nil {
			return err
		}
		rec := record.NewLogRecord(bytes)
		switch rec.Op() {
		case record.CHECKPOINT:
			return nil
		case record.COMMIT, record.ROLLBACK:
			finishedTxs[rec.TxNum()] = true
			continue
		default:
			if !finishedTxs[rec.TxNum()] {
				rec.Undo(rm.tx)
			}
		}
	}
	return nil
}
