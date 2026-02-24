package planner

import (
	"mit.edu/dsg/godb/common"
)

// PlanNode represents the static structure of a query plan.
// It is immutable and contains schema information and the plan tree structure.
type PlanNode interface {
	// OutputSchema returns the schema of the tuples produced by this node.
	OutputSchema() []common.Type

	// Children returns the child plan nodes.
	Children() []PlanNode

	// String returns a string representation of the plan node.
	String() string
}
