package execution

import (
	"errors"
	"sync"


	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/logging"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// TableHeap represents a physical table stored as a heap file on disk.
// It handles the insertion, update, deletion, and reading of tuples, managing
// interactions with the BufferPool, LockManager, and LogManager.
type TableHeap struct {
	oid         common.ObjectID
	desc        *storage.RawTupleDesc
	bufferPool  *storage.BufferPool
	logManager  logging.LogManager
	lockManager *transaction.LockManager
	appendMutex sync.Mutex

}

// NewTableHeap creates a TableHeap and performs a metadata scan to initialize stats.
func NewTableHeap(table *catalog.Table, bufferPool *storage.BufferPool, logManager logging.LogManager, lockManager *transaction.LockManager) (*TableHeap, error) {
	// Convert catalog columns to types for RawTupleDesc
	types := make([]common.Type, len(table.Columns))
	for i, col := range table.Columns {
		types[i] = col.Type
	}
	
	desc := storage.NewRawTupleDesc(types)
	
	return &TableHeap{
		oid:         table.Oid,
		desc:        desc,
		bufferPool:  bufferPool,
		logManager:  logManager,
		lockManager: lockManager,
	}, nil
}

// StorageSchema returns the physical byte-layout descriptor of the tuples in this table.
func (tableHeap *TableHeap) StorageSchema() *storage.RawTupleDesc {
	return tableHeap.desc
}


// InsertTuple inserts a tuple into the TableHeap. It should find a free space, allocating if needed, and return the found slot.
func (tableHeap *TableHeap) InsertTuple(txn *transaction.TransactionContext, row storage.RawTuple) (common.RecordID, error) {
	tableHeap.appendMutex.Lock()
	defer tableHeap.appendMutex.Unlock()
	
	sm := tableHeap.bufferPool.StorageManager()
	file, err := sm.GetDBFile(tableHeap.oid)
	if err != nil {
		return common.RecordID{}, err
	}
	
	numPages, err := file.NumPages()
	if err != nil {
		return common.RecordID{}, err
	}
	
	// Try to find a free slot in an existing page (start from the last page for append-mostly)
	for pageNum := numPages - 1; pageNum >= 0; pageNum-- {
		pageID := common.PageID{Oid: tableHeap.oid, PageNum: int32(pageNum)}
		frame, err := tableHeap.bufferPool.GetPage(pageID)
		if err != nil {
			return common.RecordID{}, err
		}
		
		frame.PageLatch.Lock()
		hp := frame.AsHeapPage()
		
		// Check if this is an initialized heap page (RowSize > 0)
		if hp.RowSize() == 0 {
			frame.PageLatch.Unlock()
			tableHeap.bufferPool.UnpinPage(frame, false)
			continue
		}
		
		slot := hp.FindFreeSlot()
		if slot >= 0 && slot < hp.NumSlots() {
			rid := common.RecordID{PageID: pageID, Slot: int32(slot)}
			hp.MarkAllocated(rid, true)
			copy(hp.AccessTuple(rid), row)
			frame.PageLatch.Unlock()
			tableHeap.bufferPool.UnpinPage(frame, true)
			return rid, nil
		}
		
		frame.PageLatch.Unlock()
		tableHeap.bufferPool.UnpinPage(frame, false)
	}
	
	// No free slot found, allocate a new page
	newPageNum, err := file.AllocatePage(1)
	if err != nil {
		return common.RecordID{}, err
	}
	
	pageID := common.PageID{Oid: tableHeap.oid, PageNum: int32(newPageNum)}
	frame, err := tableHeap.bufferPool.GetPage(pageID)
	if err != nil {
		return common.RecordID{}, err
	}
	
	frame.PageLatch.Lock()
	
	// Initialize the new heap page
	storage.InitializeHeapPage(tableHeap.desc, frame)
	hp := frame.AsHeapPage()
	
	rid := common.RecordID{PageID: pageID, Slot: 0}
	hp.MarkAllocated(rid, true)
	copy(hp.AccessTuple(rid), row)
	
	frame.PageLatch.Unlock()
	tableHeap.bufferPool.UnpinPage(frame, true)
	
	return rid, nil
}

var ErrTupleDeleted = errors.New("tuple has been deleted")

// DeleteTuple marks a tuple as deleted in the TableHeap. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) DeleteTuple(txn *transaction.TransactionContext, rid common.RecordID) error {
	frame, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return err
	}
	
	frame.PageLatch.Lock()
	defer frame.PageLatch.Unlock()
	defer tableHeap.bufferPool.UnpinPage(frame, true)
	
	hp := frame.AsHeapPage()
	
	// Check if tuple is allocated
	if !hp.IsAllocated(rid) {
		return ErrTupleDeleted
	}
	
	// Check if already deleted
	if hp.IsDeleted(rid) {
		return ErrTupleDeleted
	}
	
	hp.MarkDeleted(rid, true)
	return nil
}

// ReadTuple reads the physical bytes of a tuple into the provided buffer. If forUpdate is true, read should acquire
// exclusive lock instead of shared. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) ReadTuple(txn *transaction.TransactionContext, rid common.RecordID, buffer []byte, forUpdate bool) error {
	frame, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return err
	}
	
	if forUpdate {
		frame.PageLatch.Lock()
		defer frame.PageLatch.Unlock()
	} else {
		frame.PageLatch.RLock()
		defer frame.PageLatch.RUnlock()
	}
	defer tableHeap.bufferPool.UnpinPage(frame, false)
	
	hp := frame.AsHeapPage()
	
	// Check if tuple is allocated
	if !hp.IsAllocated(rid) {
		return ErrTupleDeleted
	}
	
	// Check if deleted
	if hp.IsDeleted(rid) {
		return ErrTupleDeleted
	}
	
	copy(buffer, hp.AccessTuple(rid))
	return nil
}

// UpdateTuple updates a tuple in-place with new binary data. If the tuple has been deleted, return ErrTupleDeleted.
func (tableHeap *TableHeap) UpdateTuple(txn *transaction.TransactionContext, rid common.RecordID, updatedTuple storage.RawTuple) error {
	frame, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return err
	}
	
	frame.PageLatch.Lock()
	defer frame.PageLatch.Unlock()
	defer tableHeap.bufferPool.UnpinPage(frame, true)
	
	hp := frame.AsHeapPage()
	
	// Check if tuple is allocated
	if !hp.IsAllocated(rid) {
		return ErrTupleDeleted
	}
	
	// Check if deleted
	if hp.IsDeleted(rid) {
		return ErrTupleDeleted
	}
	
	copy(hp.AccessTuple(rid), updatedTuple)
	return nil
}

// VacuumPage attempts to clean up deleted slots on a specific page.
// If slots are deleted AND no transaction holds a lock on them, they are marked as free.
// This is used to reclaim space in the background.
func (tableHeap *TableHeap) VacuumPage(pageID common.PageID) error {
	frame, err := tableHeap.bufferPool.GetPage(pageID)
	if err != nil {
		return err
	}
	
	frame.PageLatch.Lock()
	defer frame.PageLatch.Unlock()
	defer tableHeap.bufferPool.UnpinPage(frame, true)
	
	hp := frame.AsHeapPage()
	
	// For each slot, if it's allocated and deleted, mark it as unallocated
	for slot := 0; slot < hp.NumSlots(); slot++ {
		rid := common.RecordID{PageID: pageID, Slot: int32(slot)}
		if hp.IsAllocated(rid) && hp.IsDeleted(rid) {
			// Mark as unallocated (this also clears the deleted bit)
			hp.MarkAllocated(rid, false)
		}
	}
	
	return nil
}

// Iterator creates a new TableHeapIterator to scan the table. It acquires the supplied lock on the table (S, X, or SIX),
// and uses the supplied byte slice to fetch tuples in the returned iterator (for zero-allocation scanning).
func (tableHeap *TableHeap) Iterator(txn *transaction.TransactionContext, mode transaction.DBLockMode, buffer []byte) (TableHeapIterator, error) {
	sm := tableHeap.bufferPool.StorageManager()
	file, err := sm.GetDBFile(tableHeap.oid)
	if err != nil {
		return TableHeapIterator{}, err
	}
	
	numPages, err := file.NumPages()
	if err != nil {
		return TableHeapIterator{}, err
	}
	
	return TableHeapIterator{
		tableHeap:    tableHeap,
		buffer:       buffer,
		numPages:     numPages,
		currentPage:  -1, // Will be incremented on first Next()
		currentSlot:  -1,
		currentFrame: nil,
	}, nil
}

// TableHeapIterator iterates over all valid (allocated and non-deleted) tuples in the heap.
type TableHeapIterator struct {
	tableHeap    *TableHeap
	buffer       []byte
	numPages     int
	currentPage  int
	currentSlot  int
	currentFrame *storage.PageFrame
	err          error
}

// IsNil returns true if the TableHeapIterator is the default, uninitialized value
func (it *TableHeapIterator) IsNil() bool {
	return it.tableHeap == nil
}

// Next advances the iterator to the next valid tuple.
// It manages page pins automatically (unpinning the old page when moving to a new one).
func (it *TableHeapIterator) Next() bool {
	if it.err != nil {
		return false
	}
	
	for {
		// Try to advance within the current page
		if it.currentFrame != nil {
			it.currentFrame.PageLatch.RLock()
			hp := it.currentFrame.AsHeapPage()
			numSlots := hp.NumSlots()
			
			for it.currentSlot++; it.currentSlot < numSlots; it.currentSlot++ {
				rid := common.RecordID{
					PageID: common.PageID{Oid: it.tableHeap.oid, PageNum: int32(it.currentPage)},
					Slot:   int32(it.currentSlot),
				}
				
				if hp.IsAllocated(rid) && !hp.IsDeleted(rid) {
					// Found a valid tuple, copy it to buffer
					copy(it.buffer, hp.AccessTuple(rid))
					it.currentFrame.PageLatch.RUnlock()
					return true
				}
			}
			
			it.currentFrame.PageLatch.RUnlock()
			
			// No more valid tuples on this page, unpin and move to next page
			it.tableHeap.bufferPool.UnpinPage(it.currentFrame, false)
			it.currentFrame = nil
		}
		
		// Move to next page
		it.currentPage++
		if it.currentPage >= it.numPages {
			return false
		}
		
		// Load the new page
		pageID := common.PageID{Oid: it.tableHeap.oid, PageNum: int32(it.currentPage)}
		frame, err := it.tableHeap.bufferPool.GetPage(pageID)
		if err != nil {
			it.err = err
			return false
		}
		
		// Check if this is a valid heap page
		frame.PageLatch.RLock()
		hp := frame.AsHeapPage()
		if hp.RowSize() == 0 {
			// Not a heap page, skip it
			frame.PageLatch.RUnlock()
			it.tableHeap.bufferPool.UnpinPage(frame, false)
			continue
		}
		frame.PageLatch.RUnlock()
		
		it.currentFrame = frame
		it.currentSlot = -1 // Will be incremented in the next iteration
	}
}

// CurrentTuple returns the raw bytes of the tuple at the current cursor position.
// The bytes are valid only until Next() is called again.
func (it *TableHeapIterator) CurrentTuple() storage.RawTuple {
	return it.buffer
}

// CurrentRID returns the RecordID of the current tuple.
func (it *TableHeapIterator) CurrentRID() common.RecordID {
	return common.RecordID{
		PageID: common.PageID{Oid: it.tableHeap.oid, PageNum: int32(it.currentPage)},
		Slot:   int32(it.currentSlot),
	}
}

// CurrentRID returns the first error encountered during iteration, if any.
func (it *TableHeapIterator) Error() error {
	return it.err
}


// Close releases any resources associated with the TableHeapIterator
func (it *TableHeapIterator) Close() error {
	if it.currentFrame != nil {
		it.tableHeap.bufferPool.UnpinPage(it.currentFrame, false)
		it.currentFrame = nil
	}
	return nil
}
