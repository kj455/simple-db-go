package metadata

import (
	"fmt"

	"github.com/kj455/simple-db/pkg/record"
	"github.com/kj455/simple-db/pkg/tx"
)

type IndexInfoImpl struct {
	idxName   string
	fldName   string
	tx        tx.Transaction
	tblSchema record.Schema
	idxLayout record.Layout
	si        StatInfo
}

// NewIndexInfo creates an IndexInfoImpl object for the specified index.
func NewIndexInfo(idxName, fldName string, tblSchema record.Schema, tx tx.Transaction, si StatInfo) (*IndexInfoImpl, error) {
	ii := &IndexInfoImpl{
		idxName:   idxName,
		fldName:   fldName,
		tx:        tx,
		si:        si,
		tblSchema: tblSchema,
	}
	layout, err := ii.createIdxLayout()
	if err != nil {
		return nil, err
	}
	ii.idxLayout = layout
	return ii, nil
}

// Open opens the index described by this object.
// → NewHashIndexFromMetadata
// func (ii *IndexInfoImpl) Open() Index {
// 	return NewHashIndex(ii.tx, ii.idxname, ii.idxLayout)
// 	// return NewBTreeIndex(ii.tx, ii.idxname, ii.idxLayout)
// }

func (ii *IndexInfoImpl) IndexName() string {
	return ii.idxName
}

func (ii *IndexInfoImpl) IdxLayout() record.Layout {
	return ii.idxLayout
}

func (ii *IndexInfoImpl) IndexTx() tx.Transaction {
	return ii.tx
}

func (ii *IndexInfoImpl) Si() StatInfo {
	return ii.si
}

// BlocksAccessed estimates the number of block accesses required to find all index records.
// FIXME
func (ii *IndexInfoImpl) BlocksAccessed() int {
	rpb := ii.tx.BlockSize() / ii.idxLayout.SlotSize()
	numblocks := ii.si.RecordsOutput() / rpb
	return numblocks
	// return HashIndexSearchCost(numblocks, rpb)
	// return BTreeIndexSearchCost(numblocks, rpb)
}

// RecordsOutput returns the estimated number of records having a search key.
func (ii *IndexInfoImpl) RecordsOutput() int {
	return ii.si.RecordsOutput() / ii.si.DistinctValues(ii.fldName)
}

// DistinctValues returns the distinct values for a specified field or 1 for the indexed field.
func (ii *IndexInfoImpl) DistinctValues(fname string) int {
	if ii.fldName == fname {
		return 1
	}
	return ii.si.DistinctValues(ii.fldName)
}

// createIdxLayout returns the layout of the index records.
func (ii *IndexInfoImpl) createIdxLayout() (record.Layout, error) {
	sch := record.NewSchema()
	sch.AddIntField("block")
	sch.AddIntField("id")

	schType, err := ii.tblSchema.Type(ii.fldName)
	if err != nil {
		return nil, fmt.Errorf("metadata: failed to get field type: %v", err)
	}

	if schType == record.SCHEMA_TYPE_INTEGER {
		sch.AddIntField("dataval")
	} else {
		fldlen, err := ii.tblSchema.Length(ii.fldName)
		if err != nil {
			return nil, fmt.Errorf("metadata: failed to get field length: %v", err)
		}
		sch.AddStringField("dataval", fldlen)
	}

	layout, err := record.NewLayoutFromSchema(sch)
	if err != nil {
		return nil, fmt.Errorf("metadata: failed to create layout: %v", err)
	}
	return layout, nil
}
