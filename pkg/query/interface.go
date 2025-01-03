//go:generate mkdir -p mock
//go:generate mockgen -source=./interface.go -package=mock -destination=./mock/interface.go
package query

import (
	"github.com/kj455/simple-db/pkg/constant"
	"github.com/kj455/simple-db/pkg/record"
)

type Scan interface {
	// BeforeFirst positions the scan before its first record.
	// A subsequent call to Next will return the first record.
	BeforeFirst() error

	// Next moves the scan to the next record.
	// Returns false if there is no next record.
	Next() bool

	// GetInt returns the value of the specified integer field in the current record.
	GetInt(field string) (int, error)

	// GetString returns the value of the specified string field in the current record.
	GetString(field string) (string, error)

	// GetVal returns the value of the specified field in the current record, expressed as a Constant.
	GetVal(field string) (*constant.Const, error)

	// HasField checks if the scan has the specified field.
	// The field parameter represents the name of the field.
	// Returns true if the scan has that field.
	HasField(field string) bool

	// Close closes the scan and its subscans, if any.
	Close()
}

type UpdatableScan interface {
	Scan
	SetInt(field string, val int) error
	SetString(field string, val string) error
	SetVal(field string, val *constant.Const) error
	Insert() error
	Delete() error

	GetRID() record.RID
	MoveToRID(rid record.RID) error
}

type Predicate interface {
	IsSatisfied(s Scan) (bool, error)
	String() string
	FindFieldEquivalence(field string) (string, bool)
	FindConstantEquivalence(field string) (*constant.Const, bool)
}

type Expression interface {
	Evaluate(s Scan) (*constant.Const, error)
	IsFieldName() bool
	AsConstant() *constant.Const
	AsFieldName() string
	CanApply(sch record.Schema) bool
	ToString() string
}

type PlanInfo interface {
	DistinctValues(field string) int
}
