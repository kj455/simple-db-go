package file

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBlockId(t *testing.T) {
	filename := "test"
	blknum := 1
	bid := NewBlockId(filename, blknum)
	assert.Equal(t, filename, bid.Filename())
	assert.Equal(t, blknum, bid.Number())
}

func TestBlockIdEquals(t *testing.T) {
	bid1 := NewBlockId("test", 1)
	bid2 := NewBlockId("test", 1)
	assert.True(t, bid1.Equals(bid2))
}

func TestBlockIdString(t *testing.T) {
	bid := NewBlockId("test", 1)
	assert.Equal(t, "[file test, block 1]", bid.String())
}
