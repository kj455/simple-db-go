package query

import (
	"github.com/kj455/db/pkg/constant"
	"github.com/kj455/db/pkg/record"
)

// Expression corresponds to SQL expressions.
type ExpressionImpl struct {
	val   *constant.Const
	field string
}

// NewConstantExpression creates a new expression with a constant value.
func NewConstantExpression(val *constant.Const) *ExpressionImpl {
	return &ExpressionImpl{val: val}
}

// NewFieldExpression creates a new expression with a field name.
func NewFieldExpression(field string) *ExpressionImpl {
	return &ExpressionImpl{field: field}
}

// Evaluate evaluates the expression with respect to the current constant of the specified scan.
func (e *ExpressionImpl) Evaluate(s Scan) (*constant.Const, error) {
	if e.val != nil {
		return e.val, nil
	}
	return s.GetVal(e.field)
}

// IsFieldName returns true if the expression is a field reference.
func (e *ExpressionImpl) IsFieldName() bool {
	return e.field != ""
}

// AsConstant returns the constant corresponding to a constant expression, or nil if the expression does not denote a constant.
func (e *ExpressionImpl) AsConstant() *constant.Const {
	return e.val
}

// AsFieldName returns the field name corresponding to a constant expression, or an empty string if the expression does not denote a field.
func (e *ExpressionImpl) AsFieldName() string {
	return e.field
}

// AppliesTo determines if all of the fields mentioned in this expression are contained in the specified schema.
func (e *ExpressionImpl) AppliesTo(sch record.Schema) bool {
	if e.val != nil {
		return true
	}
	return sch.HasField(e.field)
}

// ToString returns the string representation of the expression.
func (e *ExpressionImpl) ToString() string {
	if e.val != nil {
		return e.val.ToString()
	}
	return e.field
}
