package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// ProjectionExecutor evaluates a list of expressions on the input tuples
// and produces a new tuple containing the results of those expressions.
type ProjectionExecutor struct {
	plan  *planner.ProjectionNode
	child Executor

	current       storage.Tuple
	currentValues []common.Value
	err           error
}

// NewProjectionExecutor creates a new ProjectionExecutor.
func NewProjectionExecutor(plan *planner.ProjectionNode, child Executor) *ProjectionExecutor {
	return &ProjectionExecutor{
		plan:  plan,
		child: child,
	}
}

func (e *ProjectionExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *ProjectionExecutor) Init(ctx *ExecutorContext) error {
	e.current = storage.Tuple{}
	e.err = nil
	if len(e.currentValues) != len(e.plan.Expressions) {
		e.currentValues = make([]common.Value, len(e.plan.Expressions))
	}
	if err := e.child.Init(ctx); err != nil {
		e.err = err
		return err
	}
	return nil
}

func (e *ProjectionExecutor) Next() bool {
	if e.err != nil {
		return false
	}

	if !e.child.Next() {
		if err := e.child.Error(); err != nil {
			e.err = err
		}
		return false
	}

	tup := e.child.Current()
	for i, expr := range e.plan.Expressions {
		e.currentValues[i] = expr.Eval(tup)
	}
	e.current = storage.FromValues(e.currentValues...)
	return true
}

func (e *ProjectionExecutor) Current() storage.Tuple {
	return e.current
}

func (e *ProjectionExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	return e.child.Error()
}

func (e *ProjectionExecutor) Close() error {
	return e.child.Close()
}
