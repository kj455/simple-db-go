package metadata

import (
	"fmt"

	"github.com/kj455/db/pkg/record"
	"github.com/kj455/db/pkg/tx"
)

const (
	viewCatalogTable     = "viewcat"
	viewCatalogFieldName = "viewname"
	viewCatalogFieldDef  = "viewdef"
)

const MAX_VIEWDEF = 100

type ViewMgrImpl struct {
	tblMgr TableMgr
}

func NewViewMgr(isNew bool, tblMgr TableMgr, tx tx.Transaction) (ViewMgr, error) {
	vm := &ViewMgrImpl{tblMgr: tblMgr}
	if !isNew {
		return vm, nil
	}
	sch := record.NewSchema()
	sch.AddStringField(viewCatalogFieldName, MAX_NAME)
	sch.AddStringField(viewCatalogFieldDef, MAX_VIEWDEF)
	if err := tblMgr.CreateTable(viewCatalogTable, sch, tx); err != nil {
		return nil, fmt.Errorf("view manager: %w", err)
	}
	return vm, nil
}

func (vm *ViewMgrImpl) CreateView(vname, vdef string, tx tx.Transaction) error {
	layout, err := vm.tblMgr.GetLayout(viewCatalogTable, tx)
	if err != nil {
		return fmt.Errorf("view manager: create view: %w", err)
	}
	ts, err := record.NewTableScan(tx, viewCatalogTable, layout)
	if err != nil {
		return fmt.Errorf("view manager: create view: %w", err)
	}
	if err := ts.Insert(); err != nil {
		return fmt.Errorf("view manager: create view: %w", err)
	}
	if err := ts.SetString(viewCatalogFieldName, vname); err != nil {
		return fmt.Errorf("view manager: create view: %w", err)
	}
	if err := ts.SetString(viewCatalogFieldDef, vdef); err != nil {
		return fmt.Errorf("view manager: create view: %w", err)
	}
	if err := ts.SetString(viewCatalogFieldDef, vdef); err != nil {
		return fmt.Errorf("view manager: create view: %w", err)
	}
	ts.Close()
	return nil
}

func (vm *ViewMgrImpl) GetViewDef(vname string, tx tx.Transaction) (string, error) {
	var result string
	layout, err := vm.tblMgr.GetLayout(viewCatalogTable, tx)
	if err != nil {
		return "", fmt.Errorf("view manager: get view def: %w", err)
	}
	ts, err := record.NewTableScan(tx, viewCatalogTable, layout)
	if err != nil {
		return "", fmt.Errorf("view manager: get view def: %w", err)
	}
	for ts.Next() {
		name, err := ts.GetString(viewCatalogFieldName)
		if err != nil {
			return "", fmt.Errorf("view manager: get view def: %w", err)
		}
		if name != vname {
			continue
		}
		result, err = ts.GetString(viewCatalogFieldDef)
		if err != nil {
			return "", fmt.Errorf("view manager: get view def: %w", err)
		}
		break
	}
	ts.Close()
	return result, nil
}
