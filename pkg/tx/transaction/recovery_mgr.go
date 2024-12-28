package transaction

import (
	"fmt"

	"github.com/kj455/simple-db/pkg/buffer"
	buffermgr "github.com/kj455/simple-db/pkg/buffer_mgr"
	"github.com/kj455/simple-db/pkg/log"
	"github.com/kj455/simple-db/pkg/tx"
)

type RecoveryMgrImpl struct {
	logMgr log.LogMgr
	bufMgr buffermgr.BufferMgr
	tx     tx.Transaction
	txNum  int
}

func NewRecoveryMgr(tx tx.Transaction, txNum int, lm log.LogMgr, bm buffermgr.BufferMgr) (*RecoveryMgrImpl, error) {
	rm := &RecoveryMgrImpl{
		logMgr: lm,
		bufMgr: bm,
		tx:     tx,
		txNum:  txNum,
	}
	_, err := WriteStartRecordToLog(lm, txNum)
	if err != nil {
		return nil, fmt.Errorf("recovery: failed to write start record to log: %v", err)
	}
	return rm, nil
}

// Commit writes a commit record to the log and flushes the buffer
func (rm *RecoveryMgrImpl) Commit() error {
	err := rm.bufMgr.FlushAll(rm.txNum)
	if err != nil {
		return fmt.Errorf("recovery: failed to flush buffer: %v", err)
	}
	lsn, err := WriteCommitRecordToLog(rm.logMgr, rm.txNum)
	if err != nil {
		return fmt.Errorf("recovery: failed to write commit record to log: %v", err)
	}
	rm.logMgr.Flush(lsn)
	return nil
}

// Rollback rolls back the transaction by undoing log records and flushing the buffer
func (rm *RecoveryMgrImpl) Rollback() error {
	if err := rm.rollback(); err != nil {
		return fmt.Errorf("recovery: failed to rollback: %v", err)
	}
	if err := rm.bufMgr.FlushAll(rm.txNum); err != nil {
		return fmt.Errorf("recovery: failed to flush buffer: %v", err)
	}
	lsn, err := WriteRollbackRecordToLog(rm.logMgr, rm.txNum)
	if err != nil {
		return fmt.Errorf("recovery: failed to write rollback record to log: %v", err)
	}
	err = rm.logMgr.Flush(lsn)
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
	if err := rm.bufMgr.FlushAll(rm.txNum); err != nil {
		return fmt.Errorf("recovery: failed to flush buffer: %v", err)
	}
	lsn, err := WriteCommitRecordToLog(rm.logMgr, rm.txNum)
	if err != nil {
		return fmt.Errorf("recovery: failed to write commit record to log: %v", err)
	}
	rm.logMgr.Flush(lsn)
	return nil
}

// SetInt writes old value to log
func (rm *RecoveryMgrImpl) SetInt(buff buffer.Buffer, offset int, oldVal int) (int, error) {
	return WriteSetIntRecordToLog(rm.logMgr, rm.txNum, buff.Block(), offset, int(oldVal))
}

// SetString writes old value to log
func (rm *RecoveryMgrImpl) SetString(buff buffer.Buffer, offset int, oldVal string) (int, error) {
	return WriteSetStringRecordToLog(rm.logMgr, rm.txNum, buff.Block(), offset, oldVal)
}

// rollback iterates through the log records. Each time it finds a log record for that transaction, it calls the record’s undo method. It stops when it encounters the start record for that transaction.
func (rm *RecoveryMgrImpl) rollback() error {
	iter, err := rm.logMgr.Iterator()
	if err != nil {
		return err
	}
	for iter.HasNext() {
		bytes, err := iter.Next()
		if err != nil {
			return err
		}
		rec, err := NewLogRecord(bytes)
		if err != nil {
			return err
		}
		if rec.TxNum() != rm.txNum {
			continue
		}
		if rec.Op() == OP_START {
			return nil
		}
		if err := rec.Undo(rm.tx); err != nil {
			return err
		}
	}
	return nil
}

// recover reads the log until it hits a quiescent checkpoint record or reaches the end of the log, keeping a list of committed transaction numbers. It undoes uncommitted update records the same as in rollback, the difference being that it handles all uncommitted transactions, not just a specific one.
func (rm *RecoveryMgrImpl) recover() error {
	finishedTxs := make(map[int]bool)
	iter, err := rm.logMgr.Iterator()
	if err != nil {
		return err
	}
	for iter.HasNext() {
		bytes, err := iter.Next()
		if err != nil {
			return err
		}
		rec, err := NewLogRecord(bytes)
		if err != nil {
			return err
		}
		switch rec.Op() {
		case OP_CHECKPOINT:
			return nil
		case OP_COMMIT, OP_ROLLBACK:
			finishedTxs[rec.TxNum()] = true
			continue
		default:
			if finishedTxs[rec.TxNum()] {
				continue
			}
			if err := rec.Undo(rm.tx); err != nil {
				return err
			}
		}
	}
	return nil
}
