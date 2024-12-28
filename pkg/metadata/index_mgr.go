package metadata

import (
	"fmt"
	"strings"

	"github.com/kj455/simple-db/pkg/record"
	"github.com/kj455/simple-db/pkg/tx"
)

const (
	indexTable = "idxcat"

	indexFieldIndex = "indexname"
	indexFieldTable = "tablename"
	indexFieldField = "fieldname"
)

// IndexMgr is the index manager.
type IndexMgrImpl struct {
	layout   record.Layout
	tableMgr TableMgr
	statMgr  StatMgr
}

// NewIndexMgr creates the index manager. If the database is new, the indexTable table is created.
func NewIndexMgr(tableMgr TableMgr, statMgr StatMgr, tx tx.Transaction) (IndexMgr, error) {
	hasTable, err := tableMgr.HasTable(indexTable, tx)
	if err != nil {
		return nil, fmt.Errorf("metadata: failed to check for index catalog: %w", err)
	}
	if !hasTable {
		sch := record.NewSchema()
		sch.AddStringField(indexFieldIndex, MAX_NAME_LENGTH)
		sch.AddStringField(indexFieldTable, MAX_NAME_LENGTH)
		sch.AddStringField(indexFieldField, MAX_NAME_LENGTH)
		if err := tableMgr.CreateTable(indexTable, sch, tx); err != nil {
			return nil, fmt.Errorf("metadata: failed to create index catalog: %w", err)
		}
	}
	layout, err := tableMgr.GetLayout(indexTable, tx)
	if err != nil {
		return nil, fmt.Errorf("metadata: failed to get index catalog layout: %w", err)
	}
	return &IndexMgrImpl{
		layout:   layout,
		tableMgr: tableMgr,
		statMgr:  statMgr,
	}, nil
}

// CreateIndex creates an index of the specified type for the specified field.
func (im *IndexMgrImpl) CreateIndex(idxName, tableName, fieldName string, tx tx.Transaction) error {
	ts, err := record.NewTableScan(tx, indexTable, im.layout)
	if err != nil {
		return fmt.Errorf("metadata: failed to create table scan: %w", err)
	}
	defer ts.Close()
	if err := ts.Insert(); err != nil {
		return fmt.Errorf("metadata: failed to insert into index catalog: %w", err)
	}
	if err := ts.SetString(indexFieldIndex, idxName); err != nil {
		return fmt.Errorf("metadata: failed to set index name: %w", err)
	}
	if err := ts.SetString(indexFieldTable, tableName); err != nil {
		return fmt.Errorf("metadata: failed to set table name: %w", err)
	}
	if err := ts.SetString(indexFieldField, fieldName); err != nil {
		return fmt.Errorf("metadata: failed to set field name: %w", err)
	}
	return nil
}

// GetIndexInfo returns a map containing the index info for all indexes on the specified table.
func (im *IndexMgrImpl) GetIndexInfo(tblname string, tx tx.Transaction) (map[string]IndexInfo, error) {
	result := make(map[string]IndexInfo)
	ts, err := record.NewTableScan(tx, indexTable, im.layout)
	if err != nil {
		return nil, fmt.Errorf("metadata: get index info: %w", err)
	}
	defer ts.Close()
	for ts.Next() {
		name, err := ts.GetString(indexFieldTable)
		if err != nil {
			return nil, fmt.Errorf("metadata: get index info: %w", err)
		}
		if !strings.EqualFold(name, tblname) {
			continue
		}
		idxName, err := ts.GetString(indexFieldIndex)
		if err != nil {
			return nil, fmt.Errorf("metadata: get index info: %w", err)
		}
		fldName, err := ts.GetString(indexFieldField)
		if err != nil {
			return nil, fmt.Errorf("metadata: get index info: %w", err)
		}
		tblLayout, err := im.tableMgr.GetLayout(tblname, tx)
		if err != nil {
			return nil, fmt.Errorf("metadata: get index info: %w", err)
		}
		tblStatInfo, err := im.statMgr.GetStatInfo(tblname, tblLayout, tx)
		if err != nil {
			return nil, fmt.Errorf("metadata: get index info: %w", err)
		}
		indexInfo, err := NewIndexInfo(idxName, fldName, tblLayout.Schema(), tx, tblStatInfo)
		if err != nil {
			return nil, fmt.Errorf("metadata: get index info: %w", err)
		}
		result[fldName] = indexInfo
	}
	return result, nil
}
