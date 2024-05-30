package transaction

import (
	"fmt"

	"github.com/kj455/db/pkg/file"
	"github.com/kj455/db/pkg/tx"
)

type Op int

const (
	CHECKPOINT Op = iota
	START
	COMMIT
	ROLLBACK
	SET_INT
	SET_STRING
)

const OpSize = 4

type LogRecord interface {
	Op() Op
	TxNum() int
	Undo(tx tx.Transaction)
}

func NewLogRecord(bytes []byte) LogRecord {
	p := file.NewPageFromBytes(bytes)
	op := Op(p.GetInt(0))
	fmt.Println("op", op)
	switch op {
	case CHECKPOINT:
		return NewCheckpointRecord()
	case START:
		return NewStartRecord(p)
	case COMMIT:
		return NewCommitRecord(p)
	case ROLLBACK:
		return NewRollbackRecord(p)
	case SET_INT:
		return NewSetIntRecord(p)
	case SET_STRING:
		return NewSetStringRecord(p)
	default:
		return nil
	}
}
