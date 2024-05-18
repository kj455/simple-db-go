package file

import "fmt"

type BlockId struct {
	filename string
	blockNum int
}

func NewBlockId(filename string, blockNum int) *BlockId {
	return &BlockId{
		filename: filename,
		blockNum: blockNum,
	}
}

func (b *BlockId) Filename() string {
	return b.filename
}

func (b *BlockId) Number() int {
	return b.blockNum
}

func (b *BlockId) Equals(other *BlockId) bool {
	return b.filename == other.filename && b.blockNum == other.blockNum
}

func (b *BlockId) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.filename, b.blockNum)
}
