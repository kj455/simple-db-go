package file

import "bytes"

// BlockId identifies a specific block by its filename and logical block number.
type BlockId interface {
	Filename() string
	Number() int
	Equals(other BlockId) bool
	String() string
}

// Page holds the contents of a disk block.
type Page interface {
	ReadWritePage
	Contents() *bytes.Buffer
}

type ReadWritePage interface {
	ReadPage
	WritePage
}

type ReadPage interface {
	GetInt(offset int) uint32
	GetBytes(offset int) []byte
	GetString(offset int) string
}

type WritePage interface {
	SetInt(offset int, value uint32)
	SetBytes(offset int, value []byte)
	SetString(offset int, value string)
}

// FileMgr handles the actual interaction with the OS file system.
type FileMgr interface {
	Read(id BlockId, p Page) error
	Write(id BlockId, p Page) error
	Append(filename string) (BlockId, error)
	Length(filename string) (int, error)
	BlockSize() int
}
