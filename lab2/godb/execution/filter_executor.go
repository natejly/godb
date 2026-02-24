package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// FilterExecutor filters tuples from its child executor based on a predicate.
type FilterExecutor struct {
	plan  *planner.FilterNode
	child Executor

	current storage.Tuple
	err     error
}

// NewFilter creates a new FilterExecutor executor.
func NewFilter(plan *planner.FilterNode, child Executor) *FilterExecutor {
	return &FilterExecutor{
		plan:  plan,
		child: child,
	}
}

func (e *FilterExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

// Init initializes the child.
func (e *FilterExecutor) Init(context *ExecutorContext) error {
	e.current = storage.Tuple{}
	e.err = nil
	if err := e.child.Init(context); err != nil {
		e.err = err
		return err
	}
	return nil
}

func (e *FilterExecutor) Next() bool {
	if e.err != nil {
		return false
	}

	for e.child.Next() {
		tup := e.child.Current()
		if planner.ExprIsTrue(e.plan.Predicate.Eval(tup)) {
			e.current = tup
			return true
		}
	}

	if err := e.child.Error(); err != nil {
		e.err = err
	}
	return false
}

func (e *FilterExecutor) Current() storage.Tuple {
	return e.current
}

func (e *FilterExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	return e.child.Error()
}

func (e *FilterExecutor) Close() error {
	return e.child.Close()
}
