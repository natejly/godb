package execution

import (
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// IndexScanExecutor executes a range scan over an index.
// It iterates through the B+Tree (or other index type) starting from a specific key
// and traversing in a specific direction (Forward or Backward).
type IndexScanExecutor struct {
	plan      *planner.IndexScanNode
	index     indexing.Index
	tableHeap *TableHeap
	ctx       *ExecutorContext

	iter    indexing.ScanIterator
	buffer  []byte
	current storage.Tuple
	err     error
}

func NewIndexScanExecutor(plan *planner.IndexScanNode, index indexing.Index, tableHeap *TableHeap) *IndexScanExecutor {
	return &IndexScanExecutor{
		plan:      plan,
		index:     index,
		tableHeap: tableHeap,
	}
}

func (e *IndexScanExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *IndexScanExecutor) Init(ctx *ExecutorContext) error {
	if e.iter != nil {
		if err := e.iter.Close(); err != nil {
			return err
		}
		e.iter = nil
	}

	e.ctx = ctx
	e.current = storage.Tuple{}
	e.err = nil
	if len(e.buffer) != e.tableHeap.StorageSchema().BytesPerTuple() {
		e.buffer = make([]byte, e.tableHeap.StorageSchema().BytesPerTuple())
	}

	iter, err := e.index.Scan(e.plan.StartKey, e.plan.Direction, ctx.GetTransaction())
	if err != nil {
		e.err = err
		return err
	}
	e.iter = iter
	return nil
}

func (e *IndexScanExecutor) Next() bool {
	if e.err != nil {
		return false
	}
	if e.iter == nil {
		return false
	}

	for e.iter.Next() {
		rid := e.iter.Value()
		if err := e.tableHeap.ReadTuple(e.ctx.GetTransaction(), rid, e.buffer, e.plan.ForUpdate); err != nil {
			if err == ErrTupleDeleted {
				continue
			}
			e.err = err
			return false
		}
		e.current = storage.FromRawTuple(e.buffer, e.tableHeap.StorageSchema(), rid)
		return true
	}

	if err := e.iter.Error(); err != nil {
		e.err = err
	}
	return false
}

func (e *IndexScanExecutor) Current() storage.Tuple {
	return e.current
}

func (e *IndexScanExecutor) Close() error {
	if e.iter == nil {
		return nil
	}
	err := e.iter.Close()
	e.iter = nil
	return err
}

func (e *IndexScanExecutor) Error() error {
	return e.err
}
