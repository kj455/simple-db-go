package file

import "fmt"

type BlockIdImpl struct {
	filename string
	blockNum int
}

func NewBlockId(filename string, blockNum int) *BlockIdImpl {
	return &BlockIdImpl{
		filename: filename,
		blockNum: blockNum,
	}
}

func (b *BlockIdImpl) Filename() string {
	return b.filename
}

func (b *BlockIdImpl) Number() int {
	return b.blockNum
}

func (b *BlockIdImpl) Equals(other BlockId) bool {
	return b.filename == other.Filename() && b.blockNum == other.Number()
}

func (b *BlockIdImpl) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.filename, b.blockNum)
}
