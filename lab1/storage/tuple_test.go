package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"mit.edu/dsg/godb/common"
)

func TestTupleFromValues(t *testing.T) {
	val1 := common.NewIntValue(1)
	val2 := common.NewStringValue("hello")
	tup := FromValues(val1, val2)

	assert.Equal(t, 2, tup.NumColumns())
	assert.Equal(t, val1, tup.GetValue(0))
	assert.Equal(t, val2, tup.GetValue(1))
	rid := tup.RID()
	assert.True(t, rid.IsNil(), "Virtual tuple should have nil RID")
}

func TestTupleFromRaw(t *testing.T) {
	desc := NewRawTupleDesc([]common.Type{common.IntType, common.StringType})

	buf := make([]byte, desc.BytesPerTuple())
	expectedInt := int64(42)
	expectedStr := "world"

	desc.SetValue(buf, 0, common.NewIntValue(expectedInt))
	desc.SetValue(buf, 1, common.NewStringValue(expectedStr))

	rid := common.RecordID{PageID: common.PageID{Oid: 1, PageNum: 1}, Slot: 0}
	tup := FromRawTuple(buf, desc, rid)
	assert.Equal(t, 2, tup.NumColumns())
	intValue := tup.GetValue(0)
	assert.Equal(t, expectedInt, intValue.IntValue())
	strValue := tup.GetValue(1)
	assert.Equal(t, expectedStr, strValue.StringValue())
	assert.Equal(t, rid, tup.RID())
}

func TestTupleExtend(t *testing.T) {
	desc := NewRawTupleDesc([]common.Type{common.IntType})
	buf := make([]byte, desc.BytesPerTuple())
	desc.SetValue(buf, 0, common.NewIntValue(100))
	baseTup := FromRawTuple(buf, desc, common.RecordID{})

	extraVal := common.NewStringValue("extended")
	extendedTup := baseTup.Extend([]common.Value{extraVal})

	assert.Equal(t, 2, extendedTup.NumColumns())
	intValue := extendedTup.GetValue(0)
	assert.Equal(t, int64(100), intValue.IntValue())
	strValue := extendedTup.GetValue(1)
	assert.Equal(t, "extended", strValue.StringValue())
}

func TestMergeTuples(t *testing.T) {
	descLeft := NewRawTupleDesc([]common.Type{common.IntType})
	bufLeft := make([]byte, descLeft.BytesPerTuple())
	descLeft.SetValue(bufLeft, 0, common.NewIntValue(10))
	leftTup := FromRawTuple(bufLeft, descLeft, common.RecordID{})

	rightTup := FromValues(common.NewStringValue("right"))
	descMerged := NewRawTupleDesc([]common.Type{common.IntType, common.StringType})
	bufMerged := make([]byte, descMerged.BytesPerTuple())

	resultTup := MergeTuples(bufMerged, descMerged, leftTup, rightTup)
	assert.Equal(t, 2, resultTup.NumColumns())
	intValue := resultTup.GetValue(0)
	assert.Equal(t, int64(10), intValue.IntValue())
	strValue := resultTup.GetValue(1)
	assert.Equal(t, "right", strValue.StringValue())

	val0 := descMerged.GetValue(bufMerged, 0)
	val1 := descMerged.GetValue(bufMerged, 1)
	assert.Equal(t, int64(10), val0.IntValue())
	assert.Equal(t, "right", val1.StringValue())
}

func TestDeepCopy(t *testing.T) {
	descPhys := NewRawTupleDesc([]common.Type{common.IntType})
	buf := make([]byte, descPhys.BytesPerTuple())
	descPhys.SetValue(buf, 0, common.NewIntValue(99))

	base := FromRawTuple(buf, descPhys, common.RecordID{Slot: 5})
	hybrid := base.Extend([]common.Value{common.NewStringValue("copy-me")})

	descFull := NewRawTupleDesc([]common.Type{common.IntType, common.StringType})

	copied := hybrid.DeepCopy(descFull)

	intValue := copied.GetValue(0)
	assert.Equal(t, int64(99), intValue.IntValue())
	strValue := copied.GetValue(1)
	assert.Equal(t, "copy-me", strValue.StringValue())

	descPhys.SetValue(buf, 0, common.NewIntValue(0))
	assert.Equal(t, int64(99), intValue.IntValue())
	assert.Equal(t, int32(5), copied.RID().Slot)
}

func TestWriteToBuffer(t *testing.T) {
	val1 := common.NewIntValue(123)
	val2 := common.NewStringValue("serialize")
	tup := FromValues(val1, val2)
	desc := NewRawTupleDesc([]common.Type{common.IntType, common.StringType})
	buf := make([]byte, desc.BytesPerTuple())
	tup.WriteToBuffer(buf, desc)
	readVal1 := desc.GetValue(buf, 0)
	readVal2 := desc.GetValue(buf, 1)
	assert.Equal(t, int64(123), readVal1.IntValue())
	assert.Equal(t, "serialize", readVal2.StringValue())
}
