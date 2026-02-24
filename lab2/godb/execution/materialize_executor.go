package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// MaterializeExecutor acts as a pipeline barrier.
// It consumes all tuples from its child during the first execution and stores them.
// Subsequent calls to Init/Next iterate over the stored tuples.
type MaterializeExecutor struct {
	plan  *planner.MaterializeNode
	child Executor

	outputDesc *storage.RawTupleDesc
	buffered   []storage.Tuple
	built      bool
	cursor     int
	current    storage.Tuple
	err        error
}

func NewMaterializeExecutor(plan *planner.MaterializeNode, child Executor) *MaterializeExecutor {
	return &MaterializeExecutor{
		plan:       plan,
		child:      child,
		outputDesc: storage.NewRawTupleDesc(plan.OutputSchema()),
	}
}

func (e *MaterializeExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *MaterializeExecutor) Init(ctx *ExecutorContext) error {
	e.cursor = 0
	e.current = storage.Tuple{}
	e.err = nil

	if e.built {
		return nil
	}

	e.buffered = e.buffered[:0]
	if err := e.child.Init(ctx); err != nil {
		e.err = err
		return err
	}

	for e.child.Next() {
		e.buffered = append(e.buffered, e.child.Current().DeepCopy(e.outputDesc))
	}
	if err := e.child.Error(); err != nil {
		e.err = err
		return err
	}

	e.built = true
	return nil
}

func (e *MaterializeExecutor) Next() bool {
	if e.err != nil {
		return false
	}
	if e.cursor >= len(e.buffered) {
		return false
	}
	e.current = e.buffered[e.cursor]
	e.cursor++
	return true
}

func (e *MaterializeExecutor) Current() storage.Tuple {
	return e.current
}

func (e *MaterializeExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	return e.child.Error()
}

func (e *MaterializeExecutor) Close() error {
	return e.child.Close()
}
