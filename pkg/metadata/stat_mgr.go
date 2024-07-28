package metadata

import (
	"fmt"
	"sync"

	"github.com/kj455/db/pkg/record"
	"github.com/kj455/db/pkg/tx"
)

// StatMgrImpl is responsible for keeping statistical information about each table.
type StatMgrImpl struct {
	tblMgr     TableMgr
	tablestats map[string]StatInfo
	numcalls   int
	mu         sync.Mutex
}

// NewStatMgr creates the statistics manager.
func NewStatMgr(tblMgr TableMgr, tx tx.Transaction) (StatMgr, error) {
	sm := &StatMgrImpl{
		tblMgr:     tblMgr,
		tablestats: make(map[string]StatInfo),
	}
	if err := sm.refreshStatistics(tx); err != nil {
		return nil, fmt.Errorf("stat manager: %w", err)
	}
	return sm, nil
}

// GetStatInfo returns the statistical information about the specified table.
func (sm *StatMgrImpl) GetStatInfo(tblname string, layout record.Layout, tx tx.Transaction) (StatInfo, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.numcalls++
	if sm.numcalls > 100 {
		err := sm.refreshStatistics(tx)
		if err != nil {
			return nil, fmt.Errorf("stat manager: get stat info: %w", err)
		}
	}
	if si, ok := sm.tablestats[tblname]; ok {
		return si, nil
	}
	stat, err := sm.calcTableStats(tblname, layout, tx)
	if err != nil {
		return nil, fmt.Errorf("stat manager: get stat info: %w", err)
	}
	sm.tablestats[tblname] = stat
	return stat, nil
}

func (sm *StatMgrImpl) refreshStatistics(tx tx.Transaction) error {
	sm.tablestats = make(map[string]StatInfo)
	sm.numcalls = 0
	tcatlayout, err := sm.tblMgr.GetLayout("tblcat", tx)
	if err != nil {
		return err
	}
	tcat, err := record.NewTableScan(tx, "tblcat", tcatlayout)
	if err != nil {
		return err
	}
	for tcat.Next() {
		tblname, err := tcat.GetString("tblname")
		if err != nil {
			return err
		}
		layout, err := sm.tblMgr.GetLayout(tblname, tx)
		if err != nil {
			return err
		}
		si, err := sm.calcTableStats(tblname, layout, tx)
		if err != nil {
			return err
		}
		sm.tablestats[tblname] = si
	}
	tcat.Close()
	return nil
}

func (sm *StatMgrImpl) calcTableStats(tblname string, layout record.Layout, tx tx.Transaction) (StatInfo, error) {
	var numRecs, numblocks int
	ts, err := record.NewTableScan(tx, tblname, layout)
	if err != nil {
		return nil, err
	}
	for ts.Next() {
		numRecs++
		numblocks = ts.GetRid().BlockNumber() + 1
	}
	ts.Close()
	return NewStatInfo(numblocks, numRecs), nil
}
