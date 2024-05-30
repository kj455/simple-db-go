package file

import "bytes"

type BlockId interface {
	Filename() string
	Number() int
	Equals(other BlockId) bool
	String() string
}

type Page interface {
	ReadWritePage
	Contents() *bytes.Buffer
}

type FileMgr interface {
	Read(id BlockId, p Page) error
	Write(id BlockId, p Page) error
	Append(filename string) (*BlockIdImpl, error)
	Length(filename string) (int, error)
	BlockSize() int
}
