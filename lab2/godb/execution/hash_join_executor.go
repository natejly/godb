package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// HashJoinExecutor implements the hash join algorithm.
// It builds a hash table from the left child and probes it with the right child.
// It only supports Equi-Joins.
type HashJoinExecutor struct {
	plan  *planner.HashJoinNode
	left  Executor
	right Executor

	leftDesc  *storage.RawTupleDesc
	rightDesc *storage.RawTupleDesc
	outDesc   *storage.RawTupleDesc
	keyDesc   *storage.RawTupleDesc

	results []storage.Tuple
	cursor  int
	current storage.Tuple
	err     error
}

// NewHashJoinExecutor creates a new HashJoinExecutor.
func NewHashJoinExecutor(plan *planner.HashJoinNode, left Executor, right Executor) *HashJoinExecutor {
	keyTypes := make([]common.Type, len(plan.LeftKeys))
	for i, expr := range plan.LeftKeys {
		keyTypes[i] = expr.OutputType()
	}
	return &HashJoinExecutor{
		plan:      plan,
		left:      left,
		right:     right,
		leftDesc:  storage.NewRawTupleDesc(left.PlanNode().OutputSchema()),
		rightDesc: storage.NewRawTupleDesc(right.PlanNode().OutputSchema()),
		outDesc:   storage.NewRawTupleDesc(plan.OutputSchema()),
		keyDesc:   storage.NewRawTupleDesc(keyTypes),
	}
}

func (e *HashJoinExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *HashJoinExecutor) Init(ctx *ExecutorContext) error {
	e.results = e.results[:0]
	e.cursor = 0
	e.current = storage.Tuple{}
	e.err = nil

	if err := e.left.Init(ctx); err != nil {
		e.err = err
		return err
	}
	if err := e.right.Init(ctx); err != nil {
		e.err = err
		return err
	}

	type hashJoinBucket struct {
		leftTuples []storage.Tuple
	}
	ht := NewExecutionHashTable[*hashJoinBucket](e.keyDesc)

	for e.left.Next() {
		leftTup := e.left.Current()
		if hashJoinAnyNullKey(e.plan.LeftKeys, leftTup) {
			continue
		}
		key := hashJoinEvalKey(e.plan.LeftKeys, leftTup)
		bucket, ok := ht.Get(key)
		if !ok {
			bucket = &hashJoinBucket{}
			ht.Insert(key, bucket)
		}
		bucket.leftTuples = append(bucket.leftTuples, leftTup.DeepCopy(e.leftDesc))
	}
	if err := e.left.Error(); err != nil {
		e.err = err
		return err
	}

	outBuf := make([]byte, e.outDesc.BytesPerTuple())
	for e.right.Next() {
		rightTup := e.right.Current()
		if hashJoinAnyNullKey(e.plan.RightKeys, rightTup) {
			continue
		}
		key := hashJoinEvalKey(e.plan.RightKeys, rightTup)
		bucket, ok := ht.Get(key)
		if !ok {
			continue
		}

		for _, leftTup := range bucket.leftTuples {
			joined := storage.MergeTuples(outBuf, e.outDesc, leftTup, rightTup)
			e.results = append(e.results, joined.DeepCopy(e.outDesc))
		}
	}
	if err := e.right.Error(); err != nil {
		e.err = err
		return err
	}

	return nil
}

func (e *HashJoinExecutor) Next() bool {
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

func (e *HashJoinExecutor) Current() storage.Tuple {
	return e.current
}

func (e *HashJoinExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	if err := e.left.Error(); err != nil {
		return err
	}
	return e.right.Error()
}

func (e *HashJoinExecutor) Close() error {
	e.results = nil
	leftErr := e.left.Close()
	rightErr := e.right.Close()
	if leftErr != nil {
		return leftErr
	}
	return rightErr
}

func hashJoinEvalKey(exprs []planner.Expr, tup storage.Tuple) storage.Tuple {
	if len(exprs) == 0 {
		return storage.EmptyTuple
	}
	values := make([]common.Value, len(exprs))
	for i, expr := range exprs {
		values[i] = expr.Eval(tup)
	}
	return storage.FromValues(values...)
}

func hashJoinAnyNullKey(exprs []planner.Expr, tup storage.Tuple) bool {
	for _, expr := range exprs {
		if expr.Eval(tup).IsNull() {
			return true
		}
	}
	return false
}
