package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// SeqScanExecutor implements a sequential scan over a table.
type SeqScanExecutor struct {
	plan      *planner.SeqScanNode
	tableHeap *TableHeap
	ctx       *ExecutorContext

	iter    TableHeapIterator
	buffer  []byte
	current storage.Tuple
	err     error
}

// NewSeqScanExecutor creates a new SeqScanExecutor.
func NewSeqScanExecutor(plan *planner.SeqScanNode, tableHeap *TableHeap) *SeqScanExecutor {
	return &SeqScanExecutor{
		plan:      plan,
		tableHeap: tableHeap,
	}
}

func (e *SeqScanExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *SeqScanExecutor) Init(context *ExecutorContext) error {
	if !e.iter.IsNil() {
		if err := e.iter.Close(); err != nil {
			return err
		}
		e.iter = TableHeapIterator{}
	}

	e.ctx = context
	e.current = storage.Tuple{}
	e.err = nil

	if e.buffer == nil || len(e.buffer) != e.tableHeap.StorageSchema().BytesPerTuple() {
		e.buffer = make([]byte, e.tableHeap.StorageSchema().BytesPerTuple())
	}

	iter, err := e.tableHeap.Iterator(context.GetTransaction(), e.plan.Mode, e.buffer)
	if err != nil {
		e.err = err
		return err
	}
	e.iter = iter
	return nil
}

func (e *SeqScanExecutor) Next() bool {
	if e.err != nil {
		return false
	}

	if !e.iter.Next() {
		if err := e.iter.Error(); err != nil {
			e.err = err
		}
		return false
	}

	e.current = storage.FromRawTuple(e.iter.CurrentTuple(), e.tableHeap.StorageSchema(), e.iter.CurrentRID())
	return true
}

func (e *SeqScanExecutor) Current() storage.Tuple {
	return e.current
}

func (e *SeqScanExecutor) Error() error {
	return e.err
}

func (e *SeqScanExecutor) Close() error {
	if e.iter.IsNil() {
		return nil
	}
	err := e.iter.Close()
	e.iter = TableHeapIterator{}
	return err
}
