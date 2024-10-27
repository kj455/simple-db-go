package metadata

import (
	"fmt"

	"github.com/kj455/db/pkg/record"
	"github.com/kj455/db/pkg/tx"
)

type IndexInfoImpl struct {
	idxname   string
	fldname   string
	tx        tx.Transaction
	tblSchema record.Schema
	idxLayout record.Layout
	si        StatInfo
}

// NewIndexInfo creates an IndexInfoImpl object for the specified index.
func NewIndexInfo(idxname, fldname string, tblSchema record.Schema, tx tx.Transaction, si StatInfo) (IndexInfo, error) {
	ii := &IndexInfoImpl{
		idxname:   idxname,
		fldname:   fldname,
		tx:        tx,
		tblSchema: tblSchema,
		si:        si,
	}
	l, err := ii.createIdxLayout()
	if err != nil {
		return nil, fmt.Errorf("index info: %w", err)
	}
	ii.idxLayout = l
	return ii, nil
}

// Open opens the index described by this object.
// → NewHashIndexFromMetadata
// func (ii *IndexInfoImpl) Open() Index {
// 	return NewHashIndex(ii.tx, ii.idxname, ii.idxLayout)
// 	// return NewBTreeIndex(ii.tx, ii.idxname, ii.idxLayout)
// }

func (ii *IndexInfoImpl) IndexName() string {
	return ii.idxname
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

// TODO:
// BlocksAccessed estimates the number of block accesses required to find all index records.
// func (ii *IndexInfoImpl) BlocksAccessed() int {
// 	rpb := ii.tx.BlockSize() / ii.idxLayout.SlotSize()
// 	numblocks := ii.si.RecordsOutput() / rpb
// 	return HashIndexSearchCost(numblocks, rpb)
// 	// return BTreeIndexSearchCost(numblocks, rpb)
// }

// RecordsOutput returns the estimated number of records having a search key.
func (ii *IndexInfoImpl) RecordsOutput() int {
	return ii.si.RecordsOutput() / ii.si.DistinctValues(ii.fldname)
}

// DistinctValues returns the distinct values for a specified field or 1 for the indexed field.
func (ii *IndexInfoImpl) DistinctValues(fname string) int {
	if ii.fldname == fname {
		return 1
	}
	return ii.si.DistinctValues(ii.fldname)
}

// createIdxLayout returns the layout of the index records.
func (ii *IndexInfoImpl) createIdxLayout() (record.Layout, error) {
	sch := record.NewSchema()
	sch.AddIntField("block")
	sch.AddIntField("id")
	schType, err := ii.tblSchema.Type(ii.fldname)
	if err != nil {
		return nil, err
	}
	if schType == record.SCHEMA_TYPE_INTEGER {
		sch.AddIntField("dataval")
	} else {
		fldlen, err := ii.tblSchema.Length(ii.fldname)
		if err != nil {
			return nil, err
		}
		sch.AddStringField("dataval", fldlen)
	}
	return record.NewLayoutFromSchema(sch)
}
