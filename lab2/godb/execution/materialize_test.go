package execution

import (
	"testing"

	"github.com/puzpuzpuz/xsync/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

func TestMaterialize_Basic(t *testing.T) {
	bp := storage.NewBufferPool(20, storage.NewDiskStorageManager(t.TempDir()))
	sales := setupSalesTable(t, bp)

	// Insert 5 rows: IDs 0, 1, 2, 3, 4
	insertSalesData(t, sales, nil, 5)

	// Pre-compute expected tuples for verification
	// Schema: (sale_id, revenue, cost, region, category)
	expectedTuples := make([]storage.Tuple, 5)
	for i := 0; i < 5; i++ {
		region := "US"
		if i%2 != 0 {
			region = "EU"
		}
		category := "Clothing"
		if i%3 == 0 {
			category = "Electronics"
		} else if i%3 == 1 {
			category = "Books"
		}
		expectedTuples[i] = storage.FromValues(
			common.NewIntValue(int64(i)),
			common.NewIntValue(int64(i*200)),
			common.NewIntValue(int64(i*100)),
			common.NewStringValue(region),
			common.NewStringValue(category),
		)
	}

	scanNode := planner.NewSeqScanNode(sales.oid, sales.StorageSchema().GetFieldTypes(), transaction.LockModeS)
	scanExec := NewSeqScanExecutor(scanNode, sales)

	matNode := planner.NewMaterializeNode(scanNode)
	matExec := NewMaterializeExecutor(matNode, scanExec)

	// Materialize should consume the 5 rows and buffer them.
	require.NoError(t, matExec.Init(NewExecutorContext(nil)))

	count := 0
	for matExec.Next() {
		// Use tuple equality check
		assert.True(t, expectedTuples[count].Equals(matExec.Current()))
		count++
	}
	assert.Equal(t, 5, count)

	// Insert 5 more rows into the table.
	// Since insertSalesData(..., 5) inserts 0..4, the table now has duplicates (or just more rows).
	// Because Materialize buffers, it should NOT see these new rows on the second pass.
	insertSalesData(t, sales, nil, 5)

	require.NoError(t, matExec.Init(NewExecutorContext(nil)))

	count = 0
	for matExec.Next() {
		// Should match the original snapshot exactly
		assert.True(t, expectedTuples[count].Equals(matExec.Current()))
		count++
	}
	assert.Equal(t, 5, count)

	require.NoError(t, matExec.Error())
	require.NoError(t, matExec.Close())
}

func TestMaterialize_Join_Optimization(t *testing.T) {
	// Compare BNLJ disk reads with and without Materialize on the inner relation.
	// We expect Materialize to cache the inner relation in memory, resulting in significantly fewer disk reads.
	// We also verify that the query results are identical in both cases.
	numOuterTuples := 1000
	numInnerTuples := 1000

	var readsWithout int64
	var resultsWithout []storage.Tuple
	{
		rootPath := t.TempDir()
		realSm := storage.NewDiskStorageManager(rootPath)
		statsSm := &storage.StatsDBFileManager{
			Inner: realSm,
			Files: xsync.NewMapOf[common.ObjectID, *storage.StatsDBFile](),
		}
		bp := storage.NewBufferPool(5, statsSm)

		sales := setupSalesTable(t, bp)
		shipment := setupShipmentTable(t, bp)

		insertSalesData(t, sales, nil, numOuterTuples)
		insertShipmentData(t, shipment, nil, numInnerTuples)

		statsFile, ok := statsSm.Files.Load(shipment.oid)
		require.True(t, ok)
		statsFile.ReadCnt.Store(0)

		leftScan := NewSeqScanExecutor(
			planner.NewSeqScanNode(sales.oid, sales.StorageSchema().GetFieldTypes(), transaction.LockModeS),
			sales,
		)
		rightScan := NewSeqScanExecutor(
			planner.NewSeqScanNode(shipment.oid, shipment.StorageSchema().GetFieldTypes(), transaction.LockModeS),
			shipment,
		)

		joinedSchema := append(sales.StorageSchema().GetFieldTypes(), shipment.StorageSchema().GetFieldTypes()...)
		leftCol := planner.NewColumnValueExpression(0, joinedSchema, "sales.sale_id")
		rightCol := planner.NewColumnValueExpression(6, joinedSchema, "shipment.sale_id")
		predicate := planner.NewComparisonExpression(leftCol, rightCol, planner.Equal)

		joinTree := NewBlockNestedLoopJoinExecutor(planner.NewBlockNestedLoopJoinNode(
			leftScan.PlanNode(),
			rightScan.PlanNode(),
			predicate,
		), leftScan, rightScan)

		require.NoError(t, joinTree.Init(NewExecutorContext(nil)))

		schema := storage.NewRawTupleDesc(joinTree.PlanNode().OutputSchema())
		for joinTree.Next() {
			// DeepCopy to persist results for comparison
			resultsWithout = append(resultsWithout, joinTree.Current().DeepCopy(schema))
		}
		require.NoError(t, joinTree.Close())
		readsWithout = statsFile.ReadCnt.Load()
	}

	var readsWith int64
	var resultsWith []storage.Tuple
	{
		rootPath := t.TempDir()
		realSm := storage.NewDiskStorageManager(rootPath)
		statsSm := &storage.StatsDBFileManager{
			Inner: realSm,
			Files: xsync.NewMapOf[common.ObjectID, *storage.StatsDBFile](),
		}
		bp := storage.NewBufferPool(5, statsSm)

		sales := setupSalesTable(t, bp)
		shipment := setupShipmentTable(t, bp)

		insertSalesData(t, sales, nil, numOuterTuples)
		insertShipmentData(t, shipment, nil, numInnerTuples)

		statsFile, ok := statsSm.Files.Load(shipment.oid)
		require.True(t, ok)
		statsFile.ReadCnt.Store(0)

		leftScan := NewSeqScanExecutor(
			planner.NewSeqScanNode(sales.oid, sales.StorageSchema().GetFieldTypes(), transaction.LockModeS),
			sales,
		)
		rightScan := NewSeqScanExecutor(
			planner.NewSeqScanNode(shipment.oid, shipment.StorageSchema().GetFieldTypes(), transaction.LockModeS),
			shipment,
		)

		// Inject Materialize on the inner relation
		matNode := planner.NewMaterializeNode(rightScan.PlanNode())
		matExec := NewMaterializeExecutor(matNode, rightScan)

		joinedSchema := append(sales.StorageSchema().GetFieldTypes(), shipment.StorageSchema().GetFieldTypes()...)
		leftCol := planner.NewColumnValueExpression(0, joinedSchema, "sales.sale_id")
		rightCol := planner.NewColumnValueExpression(6, joinedSchema, "shipment.sale_id")
		predicate := planner.NewComparisonExpression(leftCol, rightCol, planner.Equal)

		joinTree := NewBlockNestedLoopJoinExecutor(planner.NewBlockNestedLoopJoinNode(
			leftScan.PlanNode(),
			matExec.PlanNode(),
			predicate,
		), leftScan, matExec)

		require.NoError(t, joinTree.Init(NewExecutorContext(nil)))

		schema := storage.NewRawTupleDesc(joinTree.PlanNode().OutputSchema())
		for joinTree.Next() {
			resultsWith = append(resultsWith, joinTree.Current().DeepCopy(schema))
		}
		require.NoError(t, joinTree.Close())
		readsWith = statsFile.ReadCnt.Load()
	}

	assert.Less(t, readsWith, readsWithout, "Materialize should reduce disk I/O on the inner relation")

	assert.Equal(t, len(resultsWithout), len(resultsWith))
	for i := 0; i < len(resultsWithout); i++ {
		assert.True(t, resultsWithout[i].Equals(resultsWith[i]), "Tuple mismatch at index %d", i)
	}
}
