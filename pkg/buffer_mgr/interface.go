package buffermgr

import (
	"github.com/kj455/simple-db/pkg/buffer"
	"github.com/kj455/simple-db/pkg/file"
)

/*
BufferMgr has methods to pin and unpin a page.
The method pin returns a Buffer object pinned to a page containing the specified block, and the unpin method unpins the page.
The available method returns the number of unpinned buffer pages.
And the method flushAll ensures that all pages modified by the specified transaction have been written to disk.
*/
type BufferMgr interface {
	Pin(block file.BlockId) (buffer.Buffer, error)
	Unpin(buff buffer.Buffer)
	AvailableNum() int
	FlushAll(txNum int) error
}
