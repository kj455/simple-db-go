package query

import (
	"errors"
	"fmt"
	"strings"

	"github.com/kj455/simple-db/pkg/constant"
	"github.com/kj455/simple-db/pkg/record"
)

// PredicateImpl is a Boolean combination of terms.
type PredicateImpl struct {
	terms []*Term
}

// NewPredicate creates an empty predicate, corresponding to "true".
func NewPredicate(terms ...*Term) *PredicateImpl {
	pr := &PredicateImpl{
		terms: terms,
	}
	return pr
}

// ConjoinWith modifies the predicate to be the conjunction of itself and the specified predicate.
func (p *PredicateImpl) ConjoinWith(pred *PredicateImpl) {
	p.terms = append(p.terms, pred.terms...)
}

// IsSatisfied returns true if the predicate evaluates to true with respect to the specified scan.
func (p *PredicateImpl) IsSatisfied(s Scan) (bool, error) {
	for _, t := range p.terms {
		if ok, err := t.IsSatisfied(s); !ok || err != nil {
			return false, err
		}
	}
	return true, nil
}

// ReductionFactor calculates the extent to which selecting on the predicate reduces the number of records output by a query.
func (p *PredicateImpl) ReductionFactor(plan PlanInfo) int {
	factor := 1
	for _, t := range p.terms {
		factor *= t.ReductionFactor(plan)
	}
	return factor
}

// SelectSubPred returns the subpredicate that applies to the specified schema.
func (p *PredicateImpl) SelectSubPred(sch record.Schema) (*PredicateImpl, error) {
	result := NewPredicate()
	for _, t := range p.terms {
		if t.CanApply(sch) {
			result.terms = append(result.terms, t)
		}
	}
	if len(result.terms) == 0 {
		return nil, errors.New("query: no terms in select subpredicate")
	}
	return result, nil
}

// JoinSubPred returns the subpredicate consisting of terms that apply to the union of the two specified schemas, but not to either schema separately.
func (p *PredicateImpl) JoinSubPred(sch1, sch2 record.Schema) (*PredicateImpl, error) {
	result := NewPredicate()

	newSch := record.NewSchema()
	if err := newSch.AddAll(sch1); err != nil {
		return nil, fmt.Errorf("query: failed to add schema1: %v", err)
	}
	if err := newSch.AddAll(sch2); err != nil {
		return nil, fmt.Errorf("query: failed to add schema2: %v", err)
	}

	for _, t := range p.terms {
		// ex. for a term "F1=F2", if F1 is in sch1 and F2 is in sch2, then the term applies to the join schema
		if !t.CanApply(sch1) && !t.CanApply(sch2) && t.CanApply(newSch) {
			result.terms = append(result.terms, t)
		}
	}
	if len(result.terms) == 0 {
		return nil, errors.New("query: no terms in join subpredicate")
	}
	return result, nil
}

// FindConstantEquivalence determines if there is a term of the form "F=c" where F is the specified field and c is some constant.
func (p *PredicateImpl) FindConstantEquivalence(field string) (*constant.Const, bool) {
	for _, t := range p.terms {
		if c, ok := t.FindConstantEquivalence(field); ok {
			return c, true
		}
	}
	return nil, false
}

// FindFieldEquivalence determines if there is a term of the form "F1=F2" where F1 is the specified field and F2 is another field.
func (p *PredicateImpl) FindFieldEquivalence(field string) (string, bool) {
	for _, t := range p.terms {
		if s, ok := t.FindFieldEquivalence(field); ok {
			return s, true
		}
	}
	return "", false
}

func (p *PredicateImpl) String() string {
	var terms []string
	for _, t := range p.terms {
		terms = append(terms, t.String())
	}
	return strings.Join(terms, " and ")
}
