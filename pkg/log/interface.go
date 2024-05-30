package log

type LogMgr interface {
	Append(record []byte) (int, error)
	Flush(lsn int) error
	Iterator() (LogIterator, error)
}

type LogIterator interface {
	HasNext() bool
	Next() ([]byte, error)
}
