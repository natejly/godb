package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// SortMergeJoinExecutor implements the sort-merge join algorithm.
// The planner guarantees that both children are already sorted on their join keys.
// You only need to support Equi-Joins
type SortMergeJoinExecutor struct {
	plan  *planner.SortMergeJoinNode
	left  Executor
	right Executor

	leftDesc  *storage.RawTupleDesc
	rightDesc *storage.RawTupleDesc
	outDesc   *storage.RawTupleDesc

	results []storage.Tuple
	cursor  int
	current storage.Tuple
	err     error
}

func NewSortMergeJoinExecutor(plan *planner.SortMergeJoinNode, left, right Executor) *SortMergeJoinExecutor {
	return &SortMergeJoinExecutor{
		plan:      plan,
		left:      left,
		right:     right,
		leftDesc:  storage.NewRawTupleDesc(left.PlanNode().OutputSchema()),
		rightDesc: storage.NewRawTupleDesc(right.PlanNode().OutputSchema()),
		outDesc:   storage.NewRawTupleDesc(plan.OutputSchema()),
	}
}

func (e *SortMergeJoinExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *SortMergeJoinExecutor) Init(ctx *ExecutorContext) error {
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

	leftTuples := make([]storage.Tuple, 0)
	for e.left.Next() {
		leftTuples = append(leftTuples, e.left.Current().DeepCopy(e.leftDesc))
	}
	if err := e.left.Error(); err != nil {
		e.err = err
		return err
	}

	rightTuples := make([]storage.Tuple, 0)
	for e.right.Next() {
		rightTuples = append(rightTuples, e.right.Current().DeepCopy(e.rightDesc))
	}
	if err := e.right.Error(); err != nil {
		e.err = err
		return err
	}

	e.buildResults(leftTuples, rightTuples)
	return nil
}

func (e *SortMergeJoinExecutor) Next() bool {
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

func (e *SortMergeJoinExecutor) Current() storage.Tuple {
	return e.current
}

func (e *SortMergeJoinExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	if err := e.left.Error(); err != nil {
		return err
	}
	return e.right.Error()
}

func (e *SortMergeJoinExecutor) Close() error {
	e.results = nil
	leftErr := e.left.Close()
	rightErr := e.right.Close()
	if leftErr != nil {
		return leftErr
	}
	return rightErr
}

func (e *SortMergeJoinExecutor) buildResults(leftTuples, rightTuples []storage.Tuple) {
	i, j := 0, 0
	outBuf := make([]byte, e.outDesc.BytesPerTuple())

	for i < len(leftTuples) && j < len(rightTuples) {
		cmp := e.compareKeys(leftTuples[i], rightTuples[j])
		if cmp < 0 {
			i++
			continue
		}
		if cmp > 0 {
			j++
			continue
		}

		// Identify equal-key runs on both sides (handles duplicates).
		iEnd := i + 1
		for iEnd < len(leftTuples) && e.compareLeftKeys(leftTuples[i], leftTuples[iEnd]) == 0 {
			iEnd++
		}
		jEnd := j + 1
		for jEnd < len(rightTuples) && e.compareRightKeys(rightTuples[j], rightTuples[jEnd]) == 0 {
			jEnd++
		}

		// SQL equi-join semantics: NULL keys do not match, even if physically equal.
		if !e.anyNullKeyLeft(leftTuples[i]) && !e.anyNullKeyRight(rightTuples[j]) {
			for li := i; li < iEnd; li++ {
				for rj := j; rj < jEnd; rj++ {
					joined := storage.MergeTuples(outBuf, e.outDesc, leftTuples[li], rightTuples[rj])
					e.results = append(e.results, joined.DeepCopy(e.outDesc))
				}
			}
		}

		i = iEnd
		j = jEnd
	}
}

func (e *SortMergeJoinExecutor) compareKeys(leftTup, rightTup storage.Tuple) int {
	for k := 0; k < len(e.plan.LeftKeys); k++ {
		lv := e.plan.LeftKeys[k].Eval(leftTup)
		rv := e.plan.RightKeys[k].Eval(rightTup)
		if cmp := lv.Compare(rv); cmp != 0 {
			return cmp
		}
	}
	return 0
}

func (e *SortMergeJoinExecutor) compareLeftKeys(a, b storage.Tuple) int {
	for _, key := range e.plan.LeftKeys {
		av := key.Eval(a)
		bv := key.Eval(b)
		if cmp := av.Compare(bv); cmp != 0 {
			return cmp
		}
	}
	return 0
}

func (e *SortMergeJoinExecutor) compareRightKeys(a, b storage.Tuple) int {
	for _, key := range e.plan.RightKeys {
		av := key.Eval(a)
		bv := key.Eval(b)
		if cmp := av.Compare(bv); cmp != 0 {
			return cmp
		}
	}
	return 0
}

func (e *SortMergeJoinExecutor) anyNullKeyLeft(tup storage.Tuple) bool {
	for _, key := range e.plan.LeftKeys {
		if key.Eval(tup).IsNull() {
			return true
		}
	}
	return false
}

func (e *SortMergeJoinExecutor) anyNullKeyRight(tup storage.Tuple) bool {
	for _, key := range e.plan.RightKeys {
		if key.Eval(tup).IsNull() {
			return true
		}
	}
	return false
}
