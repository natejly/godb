package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// InsertExecutor executes an INSERT query.
// It consumes tuples from its child (which could be a ValuesExecutor or a SELECT query),
// inserts them into the TableHeap, and updates all associated indexes.
//
// For this course, you may assume that the child does not read from the table you are inserting into
type InsertExecutor struct {
	plan      *planner.InsertNode
	child     Executor
	tableHeap *TableHeap
	indexes   []indexing.Index
	ctx       *ExecutorContext

	rowBuffer  []byte
	current    storage.Tuple
	err        error
	doneInsert bool
	emitted    bool
	count      int
}

func NewInsertExecutor(plan *planner.InsertNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *InsertExecutor {
	return &InsertExecutor{
		plan:      plan,
		child:     child,
		tableHeap: tableHeap,
		indexes:   indexes,
	}
}

func (e *InsertExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *InsertExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	e.current = storage.Tuple{}
	e.err = nil
	e.doneInsert = false
	e.emitted = false
	e.count = 0
	if len(e.rowBuffer) != e.tableHeap.StorageSchema().BytesPerTuple() {
		e.rowBuffer = make([]byte, e.tableHeap.StorageSchema().BytesPerTuple())
	}
	if err := e.child.Init(ctx); err != nil {
		e.err = err
		return err
	}
	return nil
}

func (e *InsertExecutor) Next() bool {
	if e.err != nil || e.emitted {
		return false
	}

	if !e.doneInsert {
		for e.child.Next() {
			tup := e.child.Current()
			tup.WriteToBuffer(e.rowBuffer, e.tableHeap.StorageSchema())

			rid, err := e.tableHeap.InsertTuple(e.ctx.GetTransaction(), e.rowBuffer)
			if err != nil {
				e.err = err
				return false
			}

			for _, idx := range e.indexes {
				keyBuf := buildIndexKeyBuffer(idx.Metadata(), tup)
				if err := idx.InsertEntry(idx.Metadata().AsKey(keyBuf), rid, e.ctx.GetTransaction()); err != nil {
					e.err = err
					return false
				}
			}
			e.count++
		}
		if err := e.child.Error(); err != nil {
			e.err = err
			return false
		}
		e.doneInsert = true
		e.current = storage.FromValues(common.NewIntValue(int64(e.count)))
	}

	e.emitted = true
	return true
}

func (e *InsertExecutor) Current() storage.Tuple {
	return e.current
}

func (e *InsertExecutor) Close() error {
	return e.child.Close()
}

func (e *InsertExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	return e.child.Error()
}

func buildIndexKeyBuffer(md *indexing.IndexMetadata, tup storage.Tuple) []byte {
	values := make([]common.Value, len(md.ProjectionList))
	for i, col := range md.ProjectionList {
		values[i] = tup.GetValue(col)
	}

	keyTuple := storage.FromValues(values...)
	buf := make([]byte, md.KeySchema.BytesPerTuple())
	keyTuple.WriteToBuffer(buf, md.KeySchema)
	return buf
}
