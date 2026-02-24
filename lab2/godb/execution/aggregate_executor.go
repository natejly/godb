package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// AggregateExecutor implements hash-based aggregation.
type AggregateExecutor struct {
	plan  *planner.AggregateNode
	child Executor

	results []storage.Tuple
	cursor  int
	current storage.Tuple
	err     error
}

func NewAggregateExecutor(plan *planner.AggregateNode, child Executor) *AggregateExecutor {
	return &AggregateExecutor{
		plan:  plan,
		child: child,
	}
}

func (e *AggregateExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *AggregateExecutor) Init(ctx *ExecutorContext) error {
	e.results = e.results[:0]
	e.cursor = 0
	e.current = storage.Tuple{}
	e.err = nil

	if err := e.child.Init(ctx); err != nil {
		e.err = err
		return err
	}

	groupTypes := make([]common.Type, len(e.plan.GroupByClause))
	for i, expr := range e.plan.GroupByClause {
		groupTypes[i] = expr.OutputType()
	}
	groupKeyDesc := storage.NewRawTupleDesc(groupTypes)
	hashTable := NewExecutionHashTable[*aggregateState](groupKeyDesc)

	for e.child.Next() {
		tup := e.child.Current()

		groupKey := e.evalGroupKey(tup)
		state, ok := hashTable.Get(groupKey)
		if !ok {
			state = newAggregateState(e.plan.AggClauses)
			hashTable.Insert(groupKey, state)
		}
		state.Update(e.plan.AggClauses, tup)
	}
	if err := e.child.Error(); err != nil {
		e.err = err
		return err
	}

	hashTable.Iterate(func(key storage.Tuple, state *aggregateState) {
		out := make([]common.Value, 0, len(e.plan.GroupByClause)+len(e.plan.AggClauses))
		for i := 0; i < len(e.plan.GroupByClause); i++ {
			out = append(out, key.GetValue(i).Copy())
		}
		for _, v := range state.values {
			out = append(out, v.Copy())
		}
		e.results = append(e.results, storage.FromValues(out...))
	})

	return nil
}

func (e *AggregateExecutor) Next() bool {
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

func (e *AggregateExecutor) Current() storage.Tuple {
	return e.current
}

func (e *AggregateExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	return e.child.Error()
}

func (e *AggregateExecutor) Close() error {
	e.results = nil
	return e.child.Close()
}

func (e *AggregateExecutor) evalGroupKey(tup storage.Tuple) storage.Tuple {
	if len(e.plan.GroupByClause) == 0 {
		return storage.EmptyTuple
	}
	values := make([]common.Value, len(e.plan.GroupByClause))
	for i, expr := range e.plan.GroupByClause {
		values[i] = expr.Eval(tup)
	}
	return storage.FromValues(values...)
}

type aggregateState struct {
	values []common.Value
	seen   []bool // true if we've seen at least one non-NULL for non-COUNT aggregates
}

func newAggregateState(clauses []planner.AggregateClause) *aggregateState {
	state := &aggregateState{
		values: make([]common.Value, len(clauses)),
		seen:   make([]bool, len(clauses)),
	}
	for i, clause := range clauses {
		switch clause.Type {
		case planner.AggCount:
			state.values[i] = common.NewIntValue(0)
			state.seen[i] = true
		default:
			state.values[i] = nullValueForType(clause.Expr.OutputType())
		}
	}
	return state
}

func (s *aggregateState) Update(clauses []planner.AggregateClause, tup storage.Tuple) {
	for i, clause := range clauses {
		v := clause.Expr.Eval(tup)

		switch clause.Type {
		case planner.AggCount:
			if v.IsNull() {
				continue
			}
			s.values[i] = common.NewIntValue(s.values[i].IntValue() + 1)
		case planner.AggSum:
			if v.IsNull() {
				continue
			}
			if !s.seen[i] {
				s.values[i] = v.Copy()
				s.seen[i] = true
				continue
			}
			s.values[i] = common.NewIntValue(s.values[i].IntValue() + v.IntValue())
		case planner.AggMin:
			if v.IsNull() {
				continue
			}
			if !s.seen[i] || v.Compare(s.values[i]) < 0 {
				s.values[i] = v.Copy()
				s.seen[i] = true
			}
		case planner.AggMax:
			if v.IsNull() {
				continue
			}
			if !s.seen[i] || v.Compare(s.values[i]) > 0 {
				s.values[i] = v.Copy()
				s.seen[i] = true
			}
		}
	}
}

func nullValueForType(t common.Type) common.Value {
	switch t {
	case common.IntType:
		return common.NewNullInt()
	case common.StringType:
		return common.NewNullString()
	default:
		return common.Value{}
	}
}
