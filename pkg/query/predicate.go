package query

import (
	"fmt"
	"strings"

	"github.com/kj455/db/pkg/constant"
	"github.com/kj455/db/pkg/record"
)

// PredicateImpl is a Boolean combination of terms.
type PredicateImpl struct {
	terms []*Term
}

const MAX_TERMS = 10

// NewPredicate creates an empty predicate, corresponding to "true".
func NewPredicate(term *Term) *PredicateImpl {
	pr := &PredicateImpl{
		terms: make([]*Term, 0, MAX_TERMS),
	}
	if term != nil {
		// pr.terms[0] = term
		pr.terms = append(pr.terms, term)
	}
	return pr
}

// NewPredicateWithTerm creates a predicate containing a single term.
func NewPredicateWithTerm(t *Term) *PredicateImpl {
	return &PredicateImpl{terms: []*Term{t}}
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
func (p *PredicateImpl) SelectSubPred(sch record.Schema) *PredicateImpl {
	result := NewPredicate(nil)
	for _, t := range p.terms {
		if t.AppliesTo(sch) {
			result.terms = append(result.terms, t)
		}
	}
	if len(result.terms) == 0 {
		return nil
	}
	return result
}

// JoinSubPred returns the subpredicate consisting of terms that apply to the union of the two specified schemas, but not to either schema separately.
func (p *PredicateImpl) JoinSubPred(sch1, sch2 record.Schema) (*PredicateImpl, error) {
	result := NewPredicate(nil)
	newsch := record.NewSchema()
	if err := newsch.AddAll(sch1); err != nil {
		return nil, fmt.Errorf("error adding schema1: %v", err)
	}
	if err := newsch.AddAll(sch2); err != nil {
		return nil, fmt.Errorf("error adding schema2: %v", err)
	}
	for _, t := range p.terms {
		if !t.AppliesTo(sch1) && !t.AppliesTo(sch2) && t.AppliesTo(newsch) {
			result.terms = append(result.terms, t)
		}
	}
	// TODO: return nil or empty predicate?
	if len(result.terms) == 0 {
		return nil, fmt.Errorf("no join subpredicate")
	}
	return result, nil
}

// EquatesWithConstant determines if there is a term of the form "F=c" where F is the specified field and c is some constant.
func (p *PredicateImpl) EquatesWithConstant(field string) (*constant.Const, bool) {
	for _, t := range p.terms {
		if c, ok := t.EquatesWithConstant(field); ok {
			return c, true
		}
	}
	return nil, false
}

// EquatesWithField determines if there is a term of the form "F1=F2" where F1 is the specified field and F2 is another field.
func (p *PredicateImpl) EquatesWithField(field string) (string, bool) {
	for _, t := range p.terms {
		if s, ok := t.EquatesWithField(field); ok {
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
