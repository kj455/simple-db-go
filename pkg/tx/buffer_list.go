package tx

import (
	"fmt"

	"github.com/kj455/simple-db/pkg/buffer"
	"github.com/kj455/simple-db/pkg/file"
)

type BufferListImpl struct {
	buffers map[file.BlockId]buffer.Buffer
	pins    []file.BlockId
	bm      buffer.BufferMgr
}

/*
BufferList manages the list of currently pinned buffers for a transaction.
A BufferList object needs to know two things:
  - which buffer is assigned to a specified block
  - how many times each block is pinned

The code uses a map to determine buffers and a list to determine pin counts.
The list contains a BlockId object as many times as it is pinned; each time the block is unpinned, one instance is removed from the list.
*/
func NewBufferList(bm buffer.BufferMgr) *BufferListImpl {
	return &BufferListImpl{
		buffers: make(map[file.BlockId]buffer.Buffer),
		pins:    make([]file.BlockId, 0),
		bm:      bm,
	}
}

// GetBuffer gets a block from the buffer list
func (bl *BufferListImpl) GetBuffer(block file.BlockId) (buffer.Buffer, bool) {
	buf, ok := bl.buffers[block]
	return buf, ok
}

// Pin pins a block in the buffer list
func (bl *BufferListImpl) Pin(block file.BlockId) error {
	buff, err := bl.bm.Pin(block)
	if err != nil {
		return fmt.Errorf("buffer list: failed to pin block %v: %w", block, err)
	}
	bl.buffers[block] = buff
	bl.pins = append(bl.pins, block)
	return nil
}

// Unpin unpins a block in the buffer list
func (bl *BufferListImpl) Unpin(block file.BlockId) {
	buff, ok := bl.buffers[block]
	if !ok {
		return
	}
	bl.bm.Unpin(buff)
	bl.unpinBlock(block)
	if !bl.hasPinnedBlock(block) {
		delete(bl.buffers, block)
	}
}

// UnpinAll unpins all blocks in the buffer list
func (bl *BufferListImpl) UnpinAll() {
	for _, block := range bl.pins {
		buff, ok := bl.buffers[block]
		if !ok {
			continue
		}
		bl.bm.Unpin(buff)
	}
	bl.buffers = make(map[file.BlockId]buffer.Buffer)
	bl.pins = make([]file.BlockId, 0)
}

func (bl *BufferListImpl) hasPinnedBlock(block file.BlockId) bool {
	for _, b := range bl.pins {
		if b.Equals(block) {
			return true
		}
	}
	return false
}

func (bl *BufferListImpl) unpinBlock(block file.BlockId) {
	for i, b := range bl.pins {
		if b.Equals(block) {
			before, after := bl.pins[:i], bl.pins[i+1:]
			bl.pins = append(before, after...)
			break
		}
	}
}
