package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// IndexNestedLoopJoinExecutor implements an index nested loop join.
// It iterates over the left child, and for each tuple, probes the index of the right table.
// The expressions given for the left tuple should have the same schema as the right index's key
type IndexNestedLoopJoinExecutor struct {
	plan           *planner.IndexNestedLoopJoinNode
	left           Executor
	rightIndex     indexing.Index
	rightTableHeap *TableHeap
	ctx            *ExecutorContext

	leftDesc  *storage.RawTupleDesc
	rightDesc *storage.RawTupleDesc
	outDesc   *storage.RawTupleDesc

	results []storage.Tuple
	cursor  int
	current storage.Tuple
	err     error
}

// NewIndexJoinExecutor creates a new IndexNestedLoopJoinExecutor.
// It assumes the left table is accessed via the provided rightIndex and rightTableHeap.
func NewIndexJoinExecutor(plan *planner.IndexNestedLoopJoinNode, left Executor, rightIndex indexing.Index, rightTableHeap *TableHeap) *IndexNestedLoopJoinExecutor {
	return &IndexNestedLoopJoinExecutor{
		plan:           plan,
		left:           left,
		rightIndex:     rightIndex,
		rightTableHeap: rightTableHeap,
		leftDesc:       storage.NewRawTupleDesc(left.PlanNode().OutputSchema()),
		rightDesc:      rightTableHeap.StorageSchema(),
		outDesc:        storage.NewRawTupleDesc(plan.OutputSchema()),
	}
}

func (e *IndexNestedLoopJoinExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *IndexNestedLoopJoinExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	e.results = e.results[:0]
	e.cursor = 0
	e.current = storage.Tuple{}
	e.err = nil

	if err := e.left.Init(ctx); err != nil {
		e.err = err
		return err
	}

	rightBuf := make([]byte, e.rightDesc.BytesPerTuple())
	outBuf := make([]byte, e.outDesc.BytesPerTuple())
	var probeRIDs []common.RecordID

	for e.left.Next() {
		leftTup := e.left.Current().DeepCopy(e.leftDesc)

		probeKey, hasNull := e.buildProbeKey(leftTup)
		if hasNull {
			continue
		}

		rids, err := e.rightIndex.ScanKey(probeKey, probeRIDs[:0], ctx.GetTransaction())
		if err != nil {
			e.err = err
			return err
		}
		probeRIDs = rids

		for _, rid := range probeRIDs {
			if err := e.rightTableHeap.ReadTuple(ctx.GetTransaction(), rid, rightBuf, e.plan.ForUpdate); err != nil {
				if err == ErrTupleDeleted {
					continue
				}
				e.err = err
				return err
			}

			rightTup := storage.FromRawTuple(rightBuf, e.rightDesc, rid)
			joined := storage.MergeTuples(outBuf, e.outDesc, leftTup, rightTup)
			e.results = append(e.results, joined.DeepCopy(e.outDesc))
		}
	}

	if err := e.left.Error(); err != nil {
		e.err = err
		return err
	}

	return nil
}

func (e *IndexNestedLoopJoinExecutor) Next() bool {
	if e.err != nil {
		return false
	}
	if e.cursor >= len(e.results) {
		return false
	}
	e.current = e.results[e.cursor]
	e.cursor++
	return true
}

func (e *IndexNestedLoopJoinExecutor) Current() storage.Tuple {
	return e.current
}

func (e *IndexNestedLoopJoinExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	return e.left.Error()
}

func (e *IndexNestedLoopJoinExecutor) Close() error {
	e.results = nil
	return e.left.Close()
}

func (e *IndexNestedLoopJoinExecutor) buildProbeKey(leftTup storage.Tuple) (indexing.Key, bool) {
	md := e.rightIndex.Metadata()
	values := make([]common.Value, len(e.plan.LeftKeys))
	for i, expr := range e.plan.LeftKeys {
		v := expr.Eval(leftTup)
		if v.IsNull() {
			return indexing.NilKey, true
		}
		values[i] = v
	}

	keyTuple := storage.FromValues(values...)
	keyBuf := make([]byte, md.KeySchema.BytesPerTuple())
	keyTuple.WriteToBuffer(keyBuf, md.KeySchema)
	return md.AsKey(keyBuf), false
}
