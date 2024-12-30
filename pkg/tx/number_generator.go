package tx

import (
	"sync"
	"sync/atomic"
)

type TxNumberGeneratorImpl struct {
	mu   sync.Mutex
	next int32
}

var txNumGen TxNumberGenerator

// NewTxNumberGenerator returns singleton instance of TxNumberGenerator
func NewTxNumberGenerator() TxNumberGenerator {
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
