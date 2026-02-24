package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// The size of block, in bytes, that the join operator is allowed to buffer
const blockSize = 1 << 15

// BlockNestedLoopJoinExecutor implements the block nested loop join algorithm.
// It loads a block of tuples from the left child into memory and then scans the right child
// to find matches. This reduces the number of times the right child is sequentially scanned.
type BlockNestedLoopJoinExecutor struct {
	plan  *planner.NestedLoopJoinNode
	left  Executor
	right Executor
	ctx   *ExecutorContext

	leftDesc  *storage.RawTupleDesc
	rightDesc *storage.RawTupleDesc
	outDesc   *storage.RawTupleDesc

	maxBlockTuples int
	outBuffer      []byte

	leftBlock     []storage.Tuple
	leftBlockIdx  int
	rightCurrent  storage.Tuple
	rightHasTuple bool

	current storage.Tuple
	err     error
}

// NewBlockNestedLoopJoinExecutor creates a new BlockNestedLoopJoinExecutor.
func NewBlockNestedLoopJoinExecutor(plan *planner.NestedLoopJoinNode, left Executor, right Executor) *BlockNestedLoopJoinExecutor {
	leftDesc := storage.NewRawTupleDesc(left.PlanNode().OutputSchema())
	rightDesc := storage.NewRawTupleDesc(right.PlanNode().OutputSchema())
	outDesc := storage.NewRawTupleDesc(plan.OutputSchema())

	maxBlockTuples := blockSize / leftDesc.BytesPerTuple()
	if maxBlockTuples < 1 {
		maxBlockTuples = 1
	}

	return &BlockNestedLoopJoinExecutor{
		plan:           plan,
		left:           left,
		right:          right,
		leftDesc:       leftDesc,
		rightDesc:      rightDesc,
		outDesc:        outDesc,
		maxBlockTuples: maxBlockTuples,
		outBuffer:      make([]byte, outDesc.BytesPerTuple()),
		leftBlock:      make([]storage.Tuple, 0, maxBlockTuples),
	}
}

func (e *BlockNestedLoopJoinExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *BlockNestedLoopJoinExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	e.leftBlock = e.leftBlock[:0]
	e.leftBlockIdx = 0
	e.rightCurrent = storage.Tuple{}
	e.rightHasTuple = false
	e.current = storage.Tuple{}
	e.err = nil

	if err := e.left.Init(ctx); err != nil {
		e.err = err
		return err
	}
	// Initialize right once so errors surface at Init and so first block can use it.
	if err := e.right.Init(ctx); err != nil {
		e.err = err
		return err
	}
	return nil
}

func (e *BlockNestedLoopJoinExecutor) Next() bool {
	if e.err != nil {
		return false
	}

	for {
		if len(e.leftBlock) == 0 {
			if !e.loadNextLeftBlock() {
				return false
			}
		}

		if !e.rightHasTuple {
			if !e.right.Next() {
				if err := e.right.Error(); err != nil {
					e.err = err
					return false
				}
				// Finished scanning the right side for this block; fetch a new block and rescan.
				e.leftBlock = e.leftBlock[:0]
				e.leftBlockIdx = 0
				continue
			}
			e.rightCurrent = e.right.Current()
			e.rightHasTuple = true
			e.leftBlockIdx = 0
		}

		for e.leftBlockIdx < len(e.leftBlock) {
			leftTuple := e.leftBlock[e.leftBlockIdx]
			e.leftBlockIdx++

			joined := storage.MergeTuples(e.outBuffer, e.outDesc, leftTuple, e.rightCurrent)
			if e.plan.Predicate == nil || planner.ExprIsTrue(e.plan.Predicate.Eval(joined)) {
				e.current = joined
				return true
			}
		}

		// Move to the next right tuple for the same left block.
		e.rightHasTuple = false
	}
}

func (e *BlockNestedLoopJoinExecutor) Current() storage.Tuple {
	return e.current
}

func (e *BlockNestedLoopJoinExecutor) Error() error {
	return e.err
}

func (e *BlockNestedLoopJoinExecutor) Close() error {
	e.leftBlock = e.leftBlock[:0]
	e.rightHasTuple = false

	leftErr := e.left.Close()
	rightErr := e.right.Close()
	if leftErr != nil {
		return leftErr
	}
	return rightErr
}

func (e *BlockNestedLoopJoinExecutor) loadNextLeftBlock() bool {
	e.leftBlock = e.leftBlock[:0]
	e.leftBlockIdx = 0
	e.rightHasTuple = false

	for len(e.leftBlock) < e.maxBlockTuples && e.left.Next() {
		// Child tuples are only valid until the next Next() call; deep copy into block memory.
		e.leftBlock = append(e.leftBlock, e.left.Current().DeepCopy(e.leftDesc))
	}

	if err := e.left.Error(); err != nil {
		e.err = err
		return false
	}
	if len(e.leftBlock) == 0 {
		return false
	}

	// Rescan the right side once per left block.
	if err := e.right.Init(e.ctx); err != nil {
		e.err = err
		return false
	}
	return true
}
