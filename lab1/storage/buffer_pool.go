package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v3"
	"mit.edu/dsg/godb/common"
)

type BufferPool struct {
	numPages       int
	storageManager DBFileManager
	frames         []*PageFrame
	pageTable      *xsync.MapOf[common.PageID, *PageFrame]
	clockHand      int
	clockMutex     sync.Mutex

	// loadingPages tracks pages currently being loaded to prevent duplicate loads
	loadingPages *xsync.MapOf[common.PageID, chan struct{}]
}

func NewBufferPool(numPages int, storageManager DBFileManager) *BufferPool {
	frames := make([]*PageFrame, numPages)
	for i := 0; i < numPages; i++ {
		frames[i] = &PageFrame{}
	}

	return &BufferPool{
		numPages:       numPages,
		storageManager: storageManager,
		frames:         frames,
		pageTable:      xsync.NewMapOf[common.PageID, *PageFrame](),
		clockHand:      0,
		loadingPages:   xsync.NewMapOf[common.PageID, chan struct{}](),
	}
}

func (bp *BufferPool) StorageManager() DBFileManager {
	return bp.storageManager
}

func (bp *BufferPool) tryPinFrame(frame *PageFrame, pageID common.PageID) (*PageFrame, bool) {
	frame.pinCount.Add(1)

	// Verify the frame still holds our page (it may be in reclaim/reload).
	frame.metaMutex.Lock()
	currentPageID := frame.pageID
	frame.metaMutex.Unlock()

	if currentPageID == pageID && !frame.evicting.Load() {
		frame.refBit.Store(true)
		return frame, true
	}

	// Reclaim won the race; undo speculative pin and retry.
	frame.pinCount.Add(-1)
	return nil, false
}

func (bp *BufferPool) GetPage(pageID common.PageID) (*PageFrame, error) {
	for {
		// Fast path: check if page is already in buffer pool
		if frame, exists := bp.pageTable.Load(pageID); exists {
			if pinnedFrame, ok := bp.tryPinFrame(frame, pageID); ok {
				return pinnedFrame, nil
			}
			continue
		}

		// Check if another goroutine is already loading this page
		if waitCh, loading := bp.loadingPages.Load(pageID); loading {
			// Wait for the other goroutine to finish loading
			<-waitCh
			// Now retry from the beginning
			continue
		}

		// Try to claim the loading slot for this page
		loadCh := make(chan struct{})
		if _, loaded := bp.loadingPages.LoadOrStore(pageID, loadCh); loaded {
			// Another goroutine beat us, retry
			continue
		}

		// We own the loading slot now. Re-check page table to avoid duplicate loads in the
		// race where the page was loaded between our miss check and LoadOrStore above.
		if frame, exists := bp.pageTable.Load(pageID); exists {
			close(loadCh)
			bp.loadingPages.Delete(pageID)
			if pinnedFrame, ok := bp.tryPinFrame(frame, pageID); ok {
				return pinnedFrame, nil
			}
			continue
		}

		// We own the loading slot - proceed with loading
		frame, err := bp.loadPage(pageID)

		// Release the loading slot
		close(loadCh)
		bp.loadingPages.Delete(pageID)

		if err != nil {
			return nil, err
		}
		return frame, nil
	}
}

func (bp *BufferPool) loadPage(pageID common.PageID) (*PageFrame, error) {
	const maxRetries = 1000

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Try to claim a frame
		frame, oldPageID, isDirty := bp.claimFrame()
		if frame == nil {
			// All frames pinned, wait and retry
			time.Sleep(time.Microsecond * 10)
			continue
		}

		// Hold PageLatch during I/O to prevent anyone from reading/writing the bytes
		frame.PageLatch.Lock()

		// Evict old page if needed
		if oldPageID.Oid != 0 {
			// IMPORTANT: Write dirty page BEFORE removing from pageTable
			// Otherwise another thread might load stale data from disk
			if isDirty {
				file, err := bp.storageManager.GetDBFile(oldPageID.Oid)
				if err != nil {
					frame.PageLatch.Unlock()
					frame.evicting.Store(false)
					frame.pinCount.Add(-1)
					return nil, err
				}
				if err := file.WritePage(int(oldPageID.PageNum), frame.Bytes[:]); err != nil {
					frame.PageLatch.Unlock()
					frame.evicting.Store(false)
					frame.pinCount.Add(-1)
					return nil, err
				}
			}
			bp.pageTable.Delete(oldPageID)
		}

		// Read new page from disk
		file, err := bp.storageManager.GetDBFile(pageID.Oid)
		if err != nil {
			frame.PageLatch.Unlock()
			frame.evicting.Store(false)
			frame.pinCount.Add(-1)
			return nil, err
		}

		err = file.ReadPage(int(pageID.PageNum), frame.Bytes[:])
		if err != nil {
			frame.PageLatch.Unlock()
			frame.evicting.Store(false)
			frame.pinCount.Add(-1)
			return nil, err
		}

		frame.PageLatch.Unlock()

		// Update frame metadata
		frame.metaMutex.Lock()
		frame.pageID = pageID
		frame.metaMutex.Unlock()

		frame.dirty.Store(false)
		// Don't set refBit on initial load - only set on subsequent accesses
		// This provides scan resistance: pages accessed only once (scans) get no second chance
		frame.refBit.Store(false)
		frame.evicting.Store(false)

		// Add to page table
		bp.pageTable.Store(pageID, frame)

		return frame, nil
	}

	return nil, fmt.Errorf("buffer pool full - all pages pinned after %d retries", maxRetries)
}

// claimFrame finds and claims an unpinned frame using Clock algorithm.
// Returns the frame (already pinned), the old pageID, and whether it was dirty.
// Returns nil if no frame is available.
func (bp *BufferPool) claimFrame() (*PageFrame, common.PageID, bool) {
	bp.clockMutex.Lock()
	defer bp.clockMutex.Unlock()

	// Clock algorithm with scan resistance:
	// Pass 1: Look for unpinned frame with refBit=false
	// Pass 2: Clear refBits and try again
	maxIter := 256
	if bp.numPages < maxIter {
		maxIter = bp.numPages
	}

	for pass := 0; pass < 2; pass++ {
		for i := 0; i < maxIter; i++ {
			idx := bp.clockHand
			bp.clockHand = (bp.clockHand + 1) % bp.numPages
			frame := bp.frames[idx]

			// Skip pinned frames
			if frame.pinCount.Load() != 0 {
				continue
			}

			// On pass 0, only evict if refBit is false (scan resistance)
			// On pass 1, clear refBit and evict
			if pass == 0 && frame.refBit.Load() {
				frame.refBit.Store(false)
				continue
			}

			// Try to claim
			if frame.pinCount.CompareAndSwap(0, 1) {
				// Block hit-path pins while this frame is being reclaimed.
				frame.evicting.Store(true)
				// If someone pinned concurrently via a stale page-table entry, abort reclaim.
				if frame.pinCount.Load() != 1 {
					frame.evicting.Store(false)
					frame.pinCount.Add(-1)
					continue
				}

				frame.metaMutex.Lock()
				oldPageID := frame.pageID
				isDirty := frame.dirty.Load()
				frame.pageID = common.PageID{}
				frame.metaMutex.Unlock()

				return frame, oldPageID, isDirty
			}
		}
	}

	return nil, common.PageID{}, false
}

func (bp *BufferPool) UnpinPage(frame *PageFrame, setDirty bool) {
	if setDirty {
		frame.dirty.Store(true)
	}
	frame.pinCount.Add(-1)
}

func (bp *BufferPool) FlushAllPages(flushedUntil common.LSN) error {
	for _, frame := range bp.frames {
		// Hold metaMutex to get consistent pageID and dirty state
		frame.metaMutex.Lock()
		pageID := frame.pageID
		isDirty := frame.dirty.Load()
		frame.metaMutex.Unlock()

		if pageID.Oid == 0 {
			continue // empty frame
		}

		if isDirty {
			file, err := bp.storageManager.GetDBFile(pageID.Oid)
			if err != nil {
				return err
			}

			// Hold RLock while reading bytes to avoid race with writers
			frame.PageLatch.RLock()
			// Re-verify pageID hasn't changed while we were getting the lock
			frame.metaMutex.Lock()
			currentPageID := frame.pageID
			frame.metaMutex.Unlock()

			if currentPageID == pageID {
				err = file.WritePage(int(pageID.PageNum), frame.Bytes[:])
				if err == nil {
					frame.dirty.Store(false)
				}
			}
			frame.PageLatch.RUnlock()

			if currentPageID != pageID {
				continue // Frame was reused, skip
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (bp *BufferPool) GetDirtyPageTableSnapshot() map[common.PageID]common.LSN {
	return make(map[common.PageID]common.LSN)
}
