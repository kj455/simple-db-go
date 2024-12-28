package plan

import (
	"fmt"

	"github.com/kj455/db/pkg/metadata"
	"github.com/kj455/db/pkg/parse"
	"github.com/kj455/db/pkg/query"
	"github.com/kj455/db/pkg/tx"
)

type BasicUpdatePlanner struct {
	mdMgr metadata.MetadataMgr
}

func NewBasicUpdatePlanner(mdMgr metadata.MetadataMgr) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{
		mdMgr: mdMgr,
	}
}

func (bp *BasicUpdatePlanner) ExecuteDelete(data parse.DeleteData, tx tx.Transaction) (int, error) {
	tablePlan, err := NewTablePlan(tx, data.Table, bp.mdMgr)
	if err != nil {
		return 0, fmt.Errorf("planner: failed to create table plan for %s: %v", data.Table, err)
	}
	selectPlan := NewSelectPlan(tablePlan, data.Pred)
	scan, err := selectPlan.Open()
	if err != nil {
		return 0, fmt.Errorf("planner: failed to open select plan: %v", err)
	}
	updateScan, ok := scan.(query.UpdatableScan)
	if !ok {
		return 0, fmt.Errorf("planner: scan does not support updates")
	}
	defer updateScan.Close()
	count := 0
	for updateScan.Next() {
		if err := updateScan.Delete(); err != nil {
			return count, fmt.Errorf("planner: failed to delete row: %v", err)
		}
		count++
	}
	return count, nil
}

func (bp *BasicUpdatePlanner) ExecuteModify(data parse.ModifyData, tx tx.Transaction) (int, error) {
	tablePlan, err := NewTablePlan(tx, data.Table, bp.mdMgr)
	if err != nil {
		return 0, fmt.Errorf("planner: failed to create table plan for %s: %v", data.Table, err)
	}
	selectPlan := NewSelectPlan(tablePlan, data.Pred)
	scan, err := selectPlan.Open()
	if err != nil {
		return 0, fmt.Errorf("planner: failed to open select plan: %v", err)
	}
	updateScan, ok := scan.(query.UpdatableScan)
	if !ok {
		return 0, fmt.Errorf("planner: scan does not support updates")
	}
	defer updateScan.Close()
	count := 0
	for updateScan.Next() {
		val, err := data.Expr.Evaluate(updateScan)
		if err != nil {
			return count, fmt.Errorf("planner: failed to evaluate expression: %v", err)
		}
		if err := updateScan.SetVal(data.Field, val); err != nil {
			return count, fmt.Errorf("planner: failed to modify row: %v", err)
		}
		count++
	}
	return count, nil
}

func (bp *BasicUpdatePlanner) ExecuteInsert(data parse.InsertData, tx tx.Transaction) (int, error) {
	tablePlan, err := NewTablePlan(tx, data.Table, bp.mdMgr)
	if err != nil {
		return 0, fmt.Errorf("planner: failed to create table plan for %s: %v", data.Table, err)
	}
	scan, err := tablePlan.Open()
	if err != nil {
		return 0, fmt.Errorf("planner: failed to open table plan: %v", err)
	}
	insertScan, ok := scan.(query.UpdatableScan)
	if !ok {
		return 0, fmt.Errorf("planner: scan does not support updates")
	}
	defer insertScan.Close()
	if err := insertScan.Insert(); err != nil {
		return 0, fmt.Errorf("planner: failed to insert row: %v", err)
	}
	idx := 0
	for _, field := range data.Fields {
		val := data.Vals[idx]
		if err := insertScan.SetVal(field, val); err != nil {
			return 0, fmt.Errorf("planner: failed to set value: %v", err)
		}
		idx++
	}
	return 1, nil
}

func (bp *BasicUpdatePlanner) ExecuteCreateTable(data parse.CreateTableData, tx tx.Transaction) (int, error) {
	return 0, bp.mdMgr.CreateTable(data.Table, data.Schema, tx)
}

func (bp *BasicUpdatePlanner) ExecuteCreateView(data parse.CreateViewData, tx tx.Transaction) (int, error) {
	return 0, bp.mdMgr.CreateView(data.ViewName, data.ViewDef(), tx)
}

func (bp *BasicUpdatePlanner) ExecuteCreateIndex(data parse.CreateIndexData, tx tx.Transaction) (int, error) {
	return 0, bp.mdMgr.CreateIndex(data.Idx, data.Table, data.Field, tx)
}
