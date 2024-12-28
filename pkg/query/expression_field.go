package query

import (
	"github.com/kj455/simple-db/pkg/constant"
	"github.com/kj455/simple-db/pkg/record"
)

type FieldExpression struct {
	field string
}

func NewFieldExpression(field string) *FieldExpression {
	return &FieldExpression{field: field}
}

func (f *FieldExpression) Evaluate(s Scan) (*constant.Const, error) {
	return s.GetVal(f.field)
}

func (f *FieldExpression) IsFieldName() bool {
	return true
}

func (f *FieldExpression) AsConstant() *constant.Const {
	return nil
}

func (f *FieldExpression) AsFieldName() string {
	return f.field
}

func (f *FieldExpression) CanApply(sch record.Schema) bool {
	return sch.HasField(f.field)
}

func (f *FieldExpression) ToString() string {
	return f.field
}
