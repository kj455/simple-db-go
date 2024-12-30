package tx

import (
	"errors"

	"github.com/kj455/simple-db/pkg/file"
)

type Op int

const (
	OP_CHECKPOINT Op = iota + 1
	OP_START
	OP_COMMIT
	OP_ROLLBACK
	OP_SET_INT
	OP_SET_STRING
)

const (
	OffsetOp    = 0
	OffsetTxNum = 4
)

type LogRecord interface {
	Op() Op
	TxNum() int
	Undo(tx Transaction) error
}

func NewLogRecord(bytes []byte) (LogRecord, error) {
	p := file.NewPageFromBytes(bytes)
	op := Op(p.GetInt(OffsetOp))
	switch op {
	case OP_CHECKPOINT:
		return NewCheckpointRecord(), nil
	case OP_START:
		return NewStartRecord(p), nil
	case OP_COMMIT:
		return NewCommitRecord(p), nil
	case OP_ROLLBACK:
		return NewRollbackRecord(p), nil
	case OP_SET_INT:
		return NewSetIntRecord(p), nil
	case OP_SET_STRING:
		return NewSetStringRecord(p), nil
	default:
		return nil, errors.New("transaction: unknown record type")
	}
}
