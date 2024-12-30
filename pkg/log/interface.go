//go:generate mkdir -p mock
//go:generate mockgen -source=./interface.go -package=mock -destination=./mock/interface.go
package log

type LogMgr interface {
	Append(record []byte) (lsn int, err error)
	Flush(lsn int) error
	Iterator() (LogIterator, error)
}

type LogIterator interface {
	HasNext() bool
	Next() ([]byte, error)
}
