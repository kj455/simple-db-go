package metadata

import (
	"fmt"

	"github.com/kj455/simple-db/pkg/record"
	"github.com/kj455/simple-db/pkg/tx"
)

const (
	tableViewCatalog = "viewcat"

	fieldViewName = "viewname"
	fieldDef      = "viewdef"

	MAX_VIEW_DEF = 100
)

var (
	ErrViewNotFound = fmt.Errorf("metadata: view not found")
)

type ViewMgrImpl struct {
	tableMgr TableMgr
}

func NewViewMgr(tableMgr TableMgr, tx tx.Transaction) (*ViewMgrImpl, error) {
	vm := &ViewMgrImpl{tableMgr: tableMgr}
	hasTable, err := tableMgr.HasTable(tableViewCatalog, tx)
	if err != nil {
		return nil, fmt.Errorf("metadata: failed to check for view catalog: %w", err)
	}
	if hasTable {
		return vm, nil
	}
	sch := record.NewSchema()
	sch.AddStringField(fieldViewName, MAX_NAME_LENGTH)
	sch.AddStringField(fieldDef, MAX_VIEW_DEF)
	if err := tableMgr.CreateTable(tableViewCatalog, sch, tx); err != nil {
		return nil, fmt.Errorf("metadata: failed to create view catalog: %w", err)
	}
	return vm, nil
}

func (vm *ViewMgrImpl) CreateView(name, def string, tx tx.Transaction) error {
	layout, err := vm.tableMgr.GetLayout(tableViewCatalog, tx)
	if err != nil {
		return fmt.Errorf("metadata: failed to get view catalog layout: %w", err)
	}
	ts, err := record.NewTableScan(tx, tableViewCatalog, layout)
	if err != nil {
		return fmt.Errorf("metadata: failed to create table scan: %w", err)
	}
	defer ts.Close()
	if err := ts.Insert(); err != nil {
		return fmt.Errorf("metadata: failed to insert into view catalog: %w", err)
	}
	if err := ts.SetString(fieldViewName, name); err != nil {
		return fmt.Errorf("metadata: failed to set view name: %w", err)
	}
	if err := ts.SetString(fieldDef, def); err != nil {
		return fmt.Errorf("metadata: failed to set view def: %w", err)
	}
	if err := ts.SetString(fieldDef, def); err != nil {
		return fmt.Errorf("metadata: failed to set view def: %w", err)
	}
	return nil
}

func (vm *ViewMgrImpl) GetViewDef(vname string, tx tx.Transaction) (string, error) {
	var result string
	layout, err := vm.tableMgr.GetLayout(tableViewCatalog, tx)
	if err != nil {
		return "", fmt.Errorf("metadata: failed to get view catalog layout: %w", err)
	}
	ts, err := record.NewTableScan(tx, tableViewCatalog, layout)
	if err != nil {
		return "", fmt.Errorf("metadata: failed to create table scan: %w", err)
	}
	defer ts.Close()
	for ts.Next() {
		name, err := ts.GetString(fieldViewName)
		if err != nil {
			return "", fmt.Errorf("metadata: failed to get view name: %w", err)
		}
		if name != vname {
			continue
		}
		result, err = ts.GetString(fieldDef)
		if err != nil {
			return "", fmt.Errorf("metadata: failed to get view def: %w", err)
		}
		break
	}
	if result == "" {
		return "", ErrViewNotFound
	}
	return result, nil
}

func (vm *ViewMgrImpl) DeleteView(vname string, tx tx.Transaction) error {
	layout, err := vm.tableMgr.GetLayout(tableViewCatalog, tx)
	if err != nil {
		return fmt.Errorf("metadata: failed to get view catalog layout: %w", err)
	}
	ts, err := record.NewTableScan(tx, tableViewCatalog, layout)
	if err != nil {
		return fmt.Errorf("metadata: failed to create table scan: %w", err)
	}
	defer ts.Close()
	for ts.Next() {
		name, err := ts.GetString(fieldViewName)
		if err != nil {
			return fmt.Errorf("metadata: failed to get view name: %w", err)
		}
		if name != vname {
			continue
		}
		if err := ts.Delete(); err != nil {
			return fmt.Errorf("metadata: failed to delete view: %w", err)
		}
		break
	}
	return nil
}
