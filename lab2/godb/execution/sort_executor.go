package execution

import (
	"sort"

	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// SortExecutor sorts the input tuples based on the provided ordering expressions.
// It is a blocking operator but uses lazy evaluation (sorts on first Next).
type SortExecutor struct {
	plan  *planner.SortNode
	child Executor

	outputDesc *storage.RawTupleDesc
	tuples     []storage.Tuple
	cursor     int
	current    storage.Tuple
	err        error
}

func NewSortExecutor(plan *planner.SortNode, child Executor) *SortExecutor {
	return &SortExecutor{
		plan:       plan,
		child:      child,
		outputDesc: storage.NewRawTupleDesc(plan.OutputSchema()),
	}
}

func (e *SortExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *SortExecutor) Init(ctx *ExecutorContext) error {
	e.cursor = 0
	e.current = storage.Tuple{}
	e.err = nil
	e.tuples = e.tuples[:0]

	if err := e.child.Init(ctx); err != nil {
		e.err = err
		return err
	}

	for e.child.Next() {
		e.tuples = append(e.tuples, e.child.Current().DeepCopy(e.outputDesc))
	}
	if err := e.child.Error(); err != nil {
		e.err = err
		return err
	}

	if len(e.plan.OrderBy) > 0 {
		sort.SliceStable(e.tuples, func(i, j int) bool {
			return compareSortTuples(e.tuples[i], e.tuples[j], e.plan.OrderBy) < 0
		})
	}

	return nil
}

func (e *SortExecutor) Next() bool {
	if e.err != nil {
		return false
	}
	if e.cursor >= len(e.tuples) {
		return false
	}
	e.current = e.tuples[e.cursor]
	e.cursor++
	return true
}

func (e *SortExecutor) Current() storage.Tuple {
	return e.current
}

func (e *SortExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	return e.child.Error()
}

func (e *SortExecutor) Close() error {
	e.tuples = nil
	return e.child.Close()
}

func compareSortTuples(a, b storage.Tuple, orderBy []planner.OrderByClause) int {
	for _, clause := range orderBy {
		av := clause.Expr.Eval(a)
		bv := clause.Expr.Eval(b)
		cmp := av.Compare(bv)
		if cmp == 0 {
			continue
		}
		if clause.Direction == planner.SortOrderDescending {
			return -cmp
		}
		return cmp
	}
	return 0
}
