package plan

import (
	"fmt"

	"github.com/kj455/db/pkg/metadata"
	"github.com/kj455/db/pkg/query"
	"github.com/kj455/db/pkg/record"
	"github.com/kj455/db/pkg/tx"
)

type TablePlan struct {
	tx     tx.Transaction
	table  string
	layout record.Layout
	stat   metadata.StatInfo
}

func NewTablePlan(tx tx.Transaction, table string, mdMgr metadata.MetadataMgr) (*TablePlan, error) {
	layout, err := mdMgr.GetLayout(table, tx)
	if err != nil {
		return nil, fmt.Errorf("plan: failed to get layout for %s: %v", table, err)
	}
	stat, err := mdMgr.GetStatInfo(table, layout, tx)
	if err != nil {
		return nil, fmt.Errorf("plan: failed to get stat info for %s: %v", table, err)
	}
	return &TablePlan{
		tx:     tx,
		table:  table,
		layout: layout,
		stat:   stat,
	}, nil
}

func (tp *TablePlan) Open() (query.Scan, error) {
	scan, err := record.NewTableScan(tp.tx, tp.table, tp.layout)
	if err != nil {
		return nil, fmt.Errorf("plan: failed to open table scan: %v", err)
	}
	return scan, nil
}

func (tp *TablePlan) BlocksAccessed() int {
	return tp.stat.BlocksAccessed()
}

func (tp *TablePlan) RecordsOutput() int {
	return tp.stat.RecordsOutput()
}

func (tp *TablePlan) DistinctValues(field string) int {
	return tp.stat.DistinctValues(field)
}

func (tp *TablePlan) Schema() record.Schema {
	return tp.layout.Schema()
}
