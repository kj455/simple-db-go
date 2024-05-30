package buffermgr

import (
	"github.com/kj455/db/pkg/buffer"
	"github.com/kj455/db/pkg/file"
)

type BufferMgr interface {
	Pin(block file.BlockId) (buffer.Buffer, error)
	Unpin(buff buffer.Buffer)
	AvailableNum() int
	FlushAll(txNum int) error
}
