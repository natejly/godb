package execution

import (
	"bytes"

	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// UpdateExecutor implements the execution logic for updating tuples in a table.
// It iterates over the tuples provided by its child executor, which represent the full value of the current row
// and its RID. It uses the expressions defined in the plan to calculate the new values for every column in the new row.
// The executor updates the table heap in-place and ensures that all relevant indexes are updated
// if the key columns have changed. It produces a single tuple containing the count of updated rows.
type UpdateExecutor struct {
	plan      *planner.UpdateNode
	tableHeap *TableHeap
	child     Executor
	indexes   []indexing.Index
	ctx       *ExecutorContext

	rowBuffer []byte
	current   storage.Tuple
	err       error
	count     int
	emitted   bool
}

func NewUpdateExecutor(plan *planner.UpdateNode, tableHeap *TableHeap, child Executor, indexes []indexing.Index) *UpdateExecutor {
	return &UpdateExecutor{
		plan:      plan,
		tableHeap: tableHeap,
		child:     child,
		indexes:   indexes,
	}
}

func (e *UpdateExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *UpdateExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	e.current = storage.Tuple{}
	e.err = nil
	e.count = 0
	e.emitted = false
	if len(e.rowBuffer) != e.tableHeap.StorageSchema().BytesPerTuple() {
		e.rowBuffer = make([]byte, e.tableHeap.StorageSchema().BytesPerTuple())
	}
	if err := e.child.Init(ctx); err != nil {
		e.err = err
		return err
	}
	return nil
}

func (e *UpdateExecutor) Next() bool {
	if e.err != nil || e.emitted {
		return false
	}

	for e.child.Next() {
		oldTup := e.child.Current()
		rid := oldTup.RID()

		newValues := make([]common.Value, len(e.plan.Expressions))
		for i, expr := range e.plan.Expressions {
			newValues[i] = expr.Eval(oldTup)
		}
		newTup := storage.FromValues(newValues...)

		for _, idx := range e.indexes {
			oldKeyBuf := buildIndexKeyBuffer(idx.Metadata(), oldTup)
			newKeyBuf := buildIndexKeyBuffer(idx.Metadata(), newTup)
			if bytes.Equal(oldKeyBuf, newKeyBuf) {
				continue
			}
			if err := idx.DeleteEntry(idx.Metadata().AsKey(oldKeyBuf), rid, e.ctx.GetTransaction()); err != nil {
				e.err = err
				return false
			}
			if err := idx.InsertEntry(idx.Metadata().AsKey(newKeyBuf), rid, e.ctx.GetTransaction()); err != nil {
				e.err = err
				return false
			}
		}

		newTup.WriteToBuffer(e.rowBuffer, e.tableHeap.StorageSchema())
		if err := e.tableHeap.UpdateTuple(e.ctx.GetTransaction(), rid, e.rowBuffer); err != nil {
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

func (e *UpdateExecutor) OutputSchema() []common.Type {
	return e.plan.OutputSchema()
}

func (e *UpdateExecutor) Current() storage.Tuple {
	return e.current
}

func (e *UpdateExecutor) Close() error {
	return e.child.Close()
}

func (e *UpdateExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	return e.child.Error()
}
