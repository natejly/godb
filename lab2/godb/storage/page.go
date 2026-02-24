package storage

import (
	"math/bits"
	"sync"
	"sync/atomic"
	"unsafe"

	"mit.edu/dsg/godb/common"
)

// <silentstrip lab1|lab2|lab3|lab4>
type pageFrameMetadata struct {
	pageID      common.PageID
	pinCount    int
	refBit      bool
	dirty       bool
	recoveryLSN common.LSN
	sync.Mutex
}

// </silentstrip>
// pageOffsetLSN is the byte offset of the LSN within the page.
const pageOffsetLSN = 0

// PageFrame represents a physical page of data in memory.
// It holds the raw bytes of the page and acts as the container for Buffer Pool management.
type PageFrame struct {
	// Bytes holds the raw physical data of the page.
	Bytes [common.PageSize]byte
	// PageLatch protects the content of the page from concurrent access.
	PageLatch sync.RWMutex

	// Metadata for buffer pool management (using atomics for lock-free hot path)
	pageID      common.PageID
	pinCount    atomic.Int32 // Atomic for fast pin/unpin
	dirty       atomic.Bool  // Atomic for fast dirty checks
	refBit      atomic.Bool  // Atomic for Clock algorithm
	evicting    atomic.Bool  // True while frame is being reclaimed/reloaded
	recoveryLSN common.LSN
	metaMutex   sync.Mutex // Protects pageID, recoveryLSN, and complex operations
}

// Detect system endianness -- compiler should statically replace this with a constant
var isBigEndian = func() bool {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xCAFE)
	return buf[0] == 0xCA
}()

// LSN atomically reads the Log Sequence Number from the page header.
func (frame *PageFrame) LSN() common.LSN {
	ptr := (*uint64)(unsafe.Pointer(&frame.Bytes[pageOffsetLSN]))
	// 2. Atomic Load (Reads raw bytes effectively)
	val := atomic.LoadUint64(ptr)
	if isBigEndian {
		val = bits.ReverseBytes64(val)
	}
	return common.LSN(val)
}

// MonotonicallyUpdateLSN atomically updates the LSN. The update is atomic and is only applied if the given lsn is
// larger than the current value.
func (frame *PageFrame) MonotonicallyUpdateLSN(lsn common.LSN) {
	ptr := (*uint64)(unsafe.Pointer(&frame.Bytes[pageOffsetLSN]))
	newVal := uint64(lsn)

	for {
		rawCurrent := atomic.LoadUint64(ptr)
		logicalCurrent := rawCurrent
		if isBigEndian {
			logicalCurrent = bits.ReverseBytes64(rawCurrent)
		}

		if newVal <= logicalCurrent {
			return
		}

		rawNew := newVal
		if isBigEndian {
			rawNew = bits.ReverseBytes64(newVal)
		}

		if atomic.CompareAndSwapUint64(ptr, rawCurrent, rawNew) {
			return
		}
	}
}
