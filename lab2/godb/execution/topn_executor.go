package execution

import (
	"container/heap"
	"sort"

	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// TopNExecutor optimizes "ORDER BY ... LIMIT N" queries.
//
// This should allow an optimized implementation that avoids sorting ALL tuples (O(M log M)).
type TopNExecutor struct {
	plan  *planner.TopNNode
	child Executor

	outputDesc *storage.RawTupleDesc
	results    []storage.Tuple
	cursor     int
	current    storage.Tuple
	err        error
}

func NewTopNExecutor(plan *planner.TopNNode, child Executor) *TopNExecutor {
	return &TopNExecutor{
		plan:       plan,
		child:      child,
		outputDesc: storage.NewRawTupleDesc(plan.OutputSchema()),
	}
}

func (e *TopNExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *TopNExecutor) Init(ctx *ExecutorContext) error {
	e.results = e.results[:0]
	e.cursor = 0
	e.current = storage.Tuple{}
	e.err = nil

	if err := e.child.Init(ctx); err != nil {
		e.err = err
		return err
	}

	if e.plan.Limit <= 0 {
		// Drain child to surface any child error semantics consistently.
		for e.child.Next() {
		}
		if err := e.child.Error(); err != nil {
			e.err = err
			return err
		}
		return nil
	}

	h := &topNHeap{
		orderBy: e.plan.OrderBy,
		items:   make([]storage.Tuple, 0, e.plan.Limit),
	}
	heap.Init(h)

	for e.child.Next() {
		tup := e.child.Current().DeepCopy(e.outputDesc)
		if h.Len() < e.plan.Limit {
			heap.Push(h, tup)
			continue
		}

		// Heap root stores the current worst tuple among the kept top-K.
		if compareSortTuples(tup, h.items[0], e.plan.OrderBy) < 0 {
			heap.Pop(h)
			heap.Push(h, tup)
		}
	}
	if err := e.child.Error(); err != nil {
		e.err = err
		return err
	}

	e.results = append(e.results, h.items...)
	sort.SliceStable(e.results, func(i, j int) bool {
		return compareSortTuples(e.results[i], e.results[j], e.plan.OrderBy) < 0
	})

	return nil
}

func (e *TopNExecutor) Next() bool {
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

func (e *TopNExecutor) Current() storage.Tuple {
	return e.current
}

func (e *TopNExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	return e.child.Error()
}

func (e *TopNExecutor) Close() error {
	e.results = nil
	return e.child.Close()
}

type topNHeap struct {
	orderBy []planner.OrderByClause
	items   []storage.Tuple
}

func (h topNHeap) Len() int { return len(h.items) }

// We want the heap root to be the "worst" tuple among the kept results,
// so we reverse the normal sort order comparator here.
func (h topNHeap) Less(i, j int) bool {
	return compareSortTuples(h.items[i], h.items[j], h.orderBy) > 0
}

func (h topNHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *topNHeap) Push(x any) {
	h.items = append(h.items, x.(storage.Tuple))
}

func (h *topNHeap) Pop() any {
	n := len(h.items)
	x := h.items[n-1]
	h.items = h.items[:n-1]
	return x
}
