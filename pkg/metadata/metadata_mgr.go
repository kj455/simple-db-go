package metadata

import (
	"github.com/kj455/simple-db/pkg/record"
	"github.com/kj455/simple-db/pkg/tx"
)

type MetadataMgrImpl struct {
	tableMgr TableMgr
	viewMgr  ViewMgr
	statMgr  StatMgr
	idxMgr   IndexMgr
}

func NewMetadataMgr(tx tx.Transaction, opts ...TableMgrOption) (MetadataMgr, error) {
	tm, err := NewTableMgr(tx, opts...)
	if err != nil {
		return nil, err
	}
	sm, err := NewStatMgr(tm, tx)
	if err != nil {
		return nil, err
	}
	im, err := NewIndexMgr(tm, sm, tx)
	if err != nil {
		return nil, err
	}
	vm, err := NewViewMgr(tm, tx)
	if err != nil {
		return nil, err
	}

	m := &MetadataMgrImpl{
		tableMgr: tm,
		viewMgr:  vm,
		statMgr:  sm,
		idxMgr:   im,
	}
	return m, nil
}

func (m *MetadataMgrImpl) CreateTable(tblname string, sch record.Schema, tx tx.Transaction) error {
	return m.tableMgr.CreateTable(tblname, sch, tx)
}

func (m *MetadataMgrImpl) GetLayout(tblname string, tx tx.Transaction) (record.Layout, error) {
	return m.tableMgr.GetLayout(tblname, tx)
}

func (m *MetadataMgrImpl) CreateView(viewname string, viewdef string, tx tx.Transaction) error {
	return m.viewMgr.CreateView(viewname, viewdef, tx)
}

func (m *MetadataMgrImpl) GetViewDef(viewname string, tx tx.Transaction) (string, error) {
	return m.viewMgr.GetViewDef(viewname, tx)
}

func (m *MetadataMgrImpl) CreateIndex(idxname string, tblname string, fldname string, tx tx.Transaction) error {
	return m.idxMgr.CreateIndex(idxname, tblname, fldname, tx)
}

func (m *MetadataMgrImpl) GetIndexInfo(tblname string, tx tx.Transaction) (map[string]IndexInfo, error) {
	return m.idxMgr.GetIndexInfo(tblname, tx)
}

func (m *MetadataMgrImpl) GetStatInfo(tblname string, layout record.Layout, tx tx.Transaction) (StatInfo, error) {
	return m.statMgr.GetStatInfo(tblname, layout, tx)
}
