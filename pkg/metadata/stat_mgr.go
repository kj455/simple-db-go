package metadata

import (
	"fmt"
	"sync"

	"github.com/kj455/simple-db/pkg/record"
	"github.com/kj455/simple-db/pkg/tx"
)

const (
	STAT_REFRESH_THRESHOLD = 100
	INIT_STAT_CAP          = 50
)

// StatMgrImpl is responsible for keeping statistical information about each table.
type StatMgrImpl struct {
	tableMgr   TableMgr
	tableStats map[string]StatInfo
	numCalls   int
	mu         sync.Mutex
}

// NewStatMgr creates the statistics manager.
func NewStatMgr(tblMgr TableMgr, tx tx.Transaction) (*StatMgrImpl, error) {
	sm := &StatMgrImpl{
		tableMgr:   tblMgr,
		tableStats: make(map[string]StatInfo, INIT_STAT_CAP),
	}
	if err := sm.refreshStatistics(tx); err != nil {
		return nil, fmt.Errorf("metadata: failed to refresh statistics: %w", err)
	}
	return sm, nil
}

// GetStatInfo returns the statistical information about the specified table.
func (sm *StatMgrImpl) GetStatInfo(tableName string, layout record.Layout, tx tx.Transaction) (StatInfo, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.numCalls++

	if sm.numCalls > STAT_REFRESH_THRESHOLD {
		err := sm.refreshStatistics(tx)
		if err != nil {
			return nil, fmt.Errorf("metadata: failed to refresh statistics: %w", err)
		}
	}
	if stat, ok := sm.tableStats[tableName]; ok {
		return stat, nil
	}
	stat, err := sm.calcTableStats(tableName, layout, tx)
	if err != nil {
		return nil, fmt.Errorf("metadata: get stat info: %w", err)
	}
	sm.tableStats[tableName] = stat
	return stat, nil
}

func (sm *StatMgrImpl) refreshStatistics(tx tx.Transaction) error {
	sm.tableStats = make(map[string]StatInfo, INIT_STAT_CAP)
	sm.numCalls = 0

	tableCatLayout, err := sm.tableMgr.GetLayout(sm.tableMgr.TableCatalog(), tx)
	if err != nil {
		return err
	}
	tcat, err := record.NewTableScan(tx, sm.tableMgr.TableCatalog(), tableCatLayout)
	if err != nil {
		return err
	}
	defer tcat.Close()

	for tcat.Next() {
		tableName, err := tcat.GetString(fieldTableName)
		if err != nil {
			return err
		}
		layout, err := sm.tableMgr.GetLayout(tableName, tx)
		if err != nil {
			return err
		}
		si, err := sm.calcTableStats(tableName, layout, tx)
		if err != nil {
			return err
		}
		sm.tableStats[tableName] = si
	}
	return nil
}

func (sm *StatMgrImpl) calcTableStats(tableName string, layout record.Layout, tx tx.Transaction) (StatInfo, error) {
	ts, err := record.NewTableScan(tx, tableName, layout)
	if err != nil {
		return nil, err
	}
	defer ts.Close()

	var numRecs, numBlocks int
	for ts.Next() {
		numRecs++
		numBlocks = ts.GetRid().BlockNumber() + 1
	}
	return NewStatInfo(numBlocks, numRecs), nil
}
