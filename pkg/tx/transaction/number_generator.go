package transaction

import (
	"sync"
	"sync/atomic"

	"github.com/kj455/simple-db/pkg/tx"
)

type TxNumberGeneratorImpl struct {
	mu   sync.Mutex
	next int32
}

var txNumGen tx.TxNumberGenerator

// NewTxNumberGenerator returns singleton instance of tx.TxNumberGenerator
func NewTxNumberGenerator() tx.TxNumberGenerator {
	if txNumGen == nil {
		txNumGen = &TxNumberGeneratorImpl{}
	}
	return txNumGen
}

func (t *TxNumberGeneratorImpl) Next() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	num := atomic.AddInt32(&t.next, 1)
	return int(num)
}
