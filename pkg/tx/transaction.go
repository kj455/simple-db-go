package tx

import "github.com/kj455/db/pkg/file"

type Transaction interface {
	Commit()
	Rollback()
	Recover()

	Pin(block file.BlockId)
	Unpin(block file.BlockId)
	GetInt(block file.BlockId, offset int) int
	GetString(block file.BlockId, offset int) string
	SetInt(block file.BlockId, offset int, val int, okToLog bool)
	SetString(block file.BlockId, offset int, val string, okToLog bool)
	AvailableBuffs() int

	Size(filename string) int
	Append(filename string) file.BlockId
	BlockSize() int
}
