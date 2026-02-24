package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// LimitExecutor limits the number of tuples returned by the child executor.
type LimitExecutor struct {
	plan  *planner.LimitNode
	child Executor

	emitted int
	current storage.Tuple
	err     error
}

func NewLimitExecutor(plan *planner.LimitNode, child Executor) *LimitExecutor {
	return &LimitExecutor{
		plan:  plan,
		child: child,
	}
}

func (e *LimitExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *LimitExecutor) Init(ctx *ExecutorContext) error {
	e.emitted = 0
	e.current = storage.Tuple{}
	e.err = nil
	if err := e.child.Init(ctx); err != nil {
		e.err = err
		return err
	}
	return nil
}

func (e *LimitExecutor) Next() bool {
	if e.err != nil {
		return false
	}
	if e.emitted >= e.plan.Limit {
		return false
	}
	if !e.child.Next() {
		if err := e.child.Error(); err != nil {
			e.err = err
		}
		return false
	}
	e.current = e.child.Current()
	e.emitted++
	return true
}

func (e *LimitExecutor) Current() storage.Tuple {
	return e.current
}

func (e *LimitExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	return e.child.Error()
}

func (e *LimitExecutor) Close() error {
	return e.child.Close()
}
