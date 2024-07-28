package metadata

import (
	"fmt"
	"strings"

	"github.com/kj455/db/pkg/record"
	"github.com/kj455/db/pkg/tx"
)

var (
	errCreateIndex = "index manager: create index: %w"
)

// IndexMgr is the index manager.
type IndexMgrImpl struct {
	layout  record.Layout
	tblmgr  TableMgr
	statmgr StatMgr
}

// NewIndexMgr creates the index manager. If the database is new, the "idxcat" table is created.
func NewIndexMgr(isnew bool, tblmgr TableMgr, statmgr StatMgr, tx tx.Transaction) (IndexMgr, error) {
	if isnew {
		sch := record.NewSchema()
		sch.AddStringField("indexname", MAX_NAME)
		sch.AddStringField("tablename", MAX_NAME)
		sch.AddStringField("fieldname", MAX_NAME)
		if err := tblmgr.CreateTable("idxcat", sch, tx); err != nil {
			return nil, fmt.Errorf("index manager: %w", err)
		}
	}
	layout, err := tblmgr.GetLayout("idxcat", tx)
	if err != nil {
		return nil, fmt.Errorf("index manager: %w", err)
	}
	return &IndexMgrImpl{
		layout:  layout,
		tblmgr:  tblmgr,
		statmgr: statmgr,
	}, nil
}

// CreateIndex creates an index of the specified type for the specified field.
func (im *IndexMgrImpl) CreateIndex(idxname, tblname, fldname string, tx tx.Transaction) error {
	ts, err := record.NewTableScan(tx, "idxcat", im.layout)
	if err != nil {
		return fmt.Errorf(errCreateIndex, err)
	}
	if err := ts.Insert(); err != nil {
		return fmt.Errorf(errCreateIndex, err)
	}
	if err := ts.SetString("indexname", idxname); err != nil {
		return fmt.Errorf(errCreateIndex, err)
	}
	if err := ts.SetString("tablename", tblname); err != nil {
		return fmt.Errorf(errCreateIndex, err)
	}
	if err := ts.SetString("fieldname", fldname); err != nil {
		return fmt.Errorf(errCreateIndex, err)
	}
	ts.Close()
	return nil
}

// GetIndexInfo returns a map containing the index info for all indexes on the specified table.
func (im *IndexMgrImpl) GetIndexInfo(tblname string, tx tx.Transaction) (map[string]IndexInfo, error) {
	result := make(map[string]IndexInfo)
	ts, err := record.NewTableScan(tx, "idxcat", im.layout)
	if err != nil {
		return nil, fmt.Errorf("index manager: get index info: %w", err)
	}
	for ts.Next() {
		name, err := ts.GetString("tablename")
		if err != nil {
			return nil, fmt.Errorf("index manager: get index info: %w", err)
		}
		if !strings.EqualFold(name, tblname) {
			continue
		}
		idxname, err := ts.GetString("indexname")
		if err != nil {
			return nil, fmt.Errorf("index manager: get index info: %w", err)
		}
		fldname, err := ts.GetString("fieldname")
		if err != nil {
			return nil, fmt.Errorf("index manager: get index info: %w", err)
		}
		tblLayout, err := im.tblmgr.GetLayout(tblname, tx)
		if err != nil {
			return nil, fmt.Errorf("index manager: get index info: %w", err)
		}
		tblsi, err := im.statmgr.GetStatInfo(tblname, tblLayout, tx)
		if err != nil {
			return nil, fmt.Errorf("index manager: get index info: %w", err)
		}
		ii, err := NewIndexInfo(idxname, fldname, tblLayout.Schema(), tx, tblsi)
		if err != nil {
			return nil, fmt.Errorf("index manager: get index info: %w", err)
		}
		result[fldname] = ii
	}
	ts.Close()
	return result, nil
}
