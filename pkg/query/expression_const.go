package query

import (
	"github.com/kj455/db/pkg/constant"
	"github.com/kj455/db/pkg/record"
)

type ConstantExpression struct {
	val *constant.Const
}

func NewConstantExpression(val *constant.Const) *ConstantExpression {
	return &ConstantExpression{val: val}
}

func (c *ConstantExpression) Evaluate(s Scan) (*constant.Const, error) {
	return c.val, nil
}

func (c *ConstantExpression) IsFieldName() bool {
	return false
}

func (c *ConstantExpression) AsConstant() *constant.Const {
	return c.val
}

func (c *ConstantExpression) AsFieldName() string {
	return ""
}

// CanApply determines if all of the fields mentioned in this expression are contained in the specified schema.
func (c *ConstantExpression) CanApply(sch record.Schema) bool {
	return true
}

func (c *ConstantExpression) ToString() string {
	return c.val.ToString()
}
