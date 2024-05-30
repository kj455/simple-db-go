package transaction

import (
	"fmt"

	"github.com/kj455/db/pkg/buffer"
	buffermgr "github.com/kj455/db/pkg/buffer_mgr"
	"github.com/kj455/db/pkg/file"
)

type BufferListImpl struct {
	buffers map[file.BlockId]buffer.Buffer
	pins    []file.BlockId
	bm      buffermgr.BufferMgr
}

func NewBufferList(bm buffermgr.BufferMgr) *BufferListImpl {
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
	bl.removeBlockFromPins(block)
	if !bl.containsBlockInPins(block) {
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

func (bl *BufferListImpl) containsBlockInPins(block file.BlockId) bool {
	for _, b := range bl.pins {
		if b.Equals(block) {
			return true
		}
	}
	return false
}

func (bl *BufferListImpl) removeBlockFromPins(block file.BlockId) {
	for i, b := range bl.pins {
		if b.Equals(block) {
			before, after := bl.pins[:i], bl.pins[i+1:]
			bl.pins = append(before, after...)
			break
		}
	}
}
