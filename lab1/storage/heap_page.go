package storage

import (
	"encoding/binary"

	"mit.edu/dsg/godb/common"
)

// HeapPage Layout:
// LSN (8) | RowSize (2) | NumSlots (2) |  NumUsed (2) | Padding (2) | allocation Bitmap | deleted Bitmap | rows
type HeapPage struct {
	*PageFrame
}

func (hp HeapPage) NumUsed() int {
	// returns the value of NumUsed in the header
	return int(binary.LittleEndian.Uint16(hp.Bytes[12:14]))
}

func (hp HeapPage) setNumUsed(numUsed int) {
	// set the value of NumUsed in the header
	binary.LittleEndian.PutUint16(hp.Bytes[12:14], uint16(numUsed))
}

func (hp HeapPage) NumSlots() int {
	// returns the value of NumSlots in the header
	return int(binary.LittleEndian.Uint16(hp.Bytes[8:10]))
}

func (hp HeapPage) RowSize() int {
	// returns the value of RowSize in the header
	return int(binary.LittleEndian.Uint16(hp.Bytes[6:8]))
}

func InitializeHeapPage(desc *RawTupleDesc, frame *PageFrame) {
	rowSize := desc.BytesPerTuple()
	numSlots := (common.PageSize - 16) / (rowSize + 2)
	for {
		bitmapSize := common.Align8(numSlots)
		availableSpace := common.PageSize - 16 - 2*bitmapSize
		maxSlots := availableSpace / rowSize
		if maxSlots >= numSlots {
			break
		}
		numSlots = maxSlots
	}
	binary.LittleEndian.PutUint16(frame.Bytes[6:8], uint16(rowSize))
	binary.LittleEndian.PutUint16(frame.Bytes[8:10], uint16(numSlots))
	binary.LittleEndian.PutUint16(frame.Bytes[12:14], 0)
}

func (frame *PageFrame) AsHeapPage() HeapPage {
	return HeapPage{frame}
}

func (hp HeapPage) FindFreeSlot() int {
	bitmapSize := common.Align8(hp.NumSlots())
	allocationBitmap := AsBitmap(hp.Bytes[16:16+bitmapSize], hp.NumSlots())
	return allocationBitmap.FindFirstZero(0)
}

// IsAllocated checks the allocation bitmap to see if a slot is valid.
func (hp HeapPage) IsAllocated(rid common.RecordID) bool {
	bitmapSize := common.Align8(hp.NumSlots())
	allocationBitmap := AsBitmap(hp.Bytes[16:16+bitmapSize], hp.NumSlots())
	return allocationBitmap.LoadBit(int(rid.Slot))
}


func (hp HeapPage) MarkAllocated(rid common.RecordID, allocated bool) {
	bitmapSize := common.Align8(hp.NumSlots())
	allocationBitmap := AsBitmap(hp.Bytes[16:16+bitmapSize], hp.NumSlots())
	wasAllocated := allocationBitmap.SetBit(int(rid.Slot), allocated)
	
	// Update NumUsed counter
	if allocated && !wasAllocated {
		// Allocating a previously free slot
		hp.setNumUsed(hp.NumUsed() + 1)
	} else if !allocated && wasAllocated {
		// Freeing a previously allocated slot
		hp.setNumUsed(hp.NumUsed() - 1)
		// Also clear the deleted bit when freeing
		deletedBitmap := AsBitmap(hp.Bytes[16+bitmapSize:16+2*bitmapSize], hp.NumSlots())
		deletedBitmap.SetBit(int(rid.Slot), false)
	}
}

func (hp HeapPage) IsDeleted(rid common.RecordID) bool {
	bitmapSize := common.Align8(hp.NumSlots())
	deletedBitmap := AsBitmap(hp.Bytes[16+bitmapSize:16+2*bitmapSize], hp.NumSlots())
	return deletedBitmap.LoadBit(int(rid.Slot))
}

func (hp HeapPage) MarkDeleted(rid common.RecordID, deleted bool) {
	bitmapSize := common.Align8(hp.NumSlots())
	deletedBitmap := AsBitmap(hp.Bytes[16+bitmapSize:16+2*bitmapSize], hp.NumSlots())
	deletedBitmap.SetBit(int(rid.Slot), deleted)
}

func (hp HeapPage) AccessTuple(rid common.RecordID) RawTuple {
	rowSize := hp.RowSize()
	bitmapSize := common.Align8(hp.NumSlots())
	offset := 16 + 2*bitmapSize + int(rid.Slot)*rowSize
	return hp.Bytes[offset : offset+rowSize]
}
