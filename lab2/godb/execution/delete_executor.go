package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// DeleteExecutor executes a DELETE query.
// It iterates over the child (which produces the tuples to be deleted with all rows read),
// removes them from the TableHeap, and cleans up all associated Index entries.
type DeleteExecutor struct {
	plan      *planner.DeleteNode
	child     Executor
	tableHeap *TableHeap
	indexes   []indexing.Index
	ctx       *ExecutorContext

	current storage.Tuple
	err     error
	count   int
	emitted bool
}

func NewDeleteExecutor(plan *planner.DeleteNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *DeleteExecutor {
	return &DeleteExecutor{
		plan:      plan,
		child:     child,
		tableHeap: tableHeap,
		indexes:   indexes,
	}
}

func (e *DeleteExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *DeleteExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	e.current = storage.Tuple{}
	e.err = nil
	e.count = 0
	e.emitted = false
	if err := e.child.Init(ctx); err != nil {
		e.err = err
		return err
	}
	return nil
}

func (e *DeleteExecutor) Next() bool {
	if e.err != nil || e.emitted {
		return false
	}

	for e.child.Next() {
		tup := e.child.Current()
		rid := tup.RID()

		for _, idx := range e.indexes {
			keyBuf := buildIndexKeyBuffer(idx.Metadata(), tup)
			if err := idx.DeleteEntry(idx.Metadata().AsKey(keyBuf), rid, e.ctx.GetTransaction()); err != nil {
				e.err = err
				return false
			}
		}

		if err := e.tableHeap.DeleteTuple(e.ctx.GetTransaction(), rid); err != nil {
			e.err = err
			return false
		}
		e.count++
	}
	if err := e.child.Error(); err != nil {
		e.err = err
		return false
	}

	e.current = storage.FromValues(common.NewIntValue(int64(e.count)))
	e.emitted = true
	return true
}

func (e *DeleteExecutor) Current() storage.Tuple {
	return e.current
}

func (e *DeleteExecutor) Close() error {
	return e.child.Close()
}

func (e *DeleteExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	return e.child.Error()
}
