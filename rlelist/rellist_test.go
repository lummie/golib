package rlelist

import (
	"github.com/lummie/golib/assert"
	"math/rand"
	"testing"
)

func TestThatBlockAndRowCountAreCorrectForAppend(t *testing.T) {
	list := New()
	assert.Equal(t, list.blockCount, 0, "Expect blockCount to be zero on empty list")
	assert.Equal(t, list.rowCount, 0, "Expect rowCount to be zero on empty list")

	var index uint
	index = list.Append("Value 1")
	assert.Equal(t, index, 0)
	assert.Equal(t, list.blockCount, 1, "Expect blockCount to be 1")
	assert.Equal(t, list.rowCount, 1, "Expect rowCount to be 1")

	index = list.Append("Value 2")
	assert.Equal(t, index, 1)
	assert.Equal(t, list.blockCount, 2, "Expect blockCount to be 2")
	assert.Equal(t, list.rowCount, 2, "Expect rowCount to be 2")

	index = list.Append("Value 2")
	assert.Equal(t, index, 2)
	assert.Equal(t, list.blockCount, 2, "Expect blockCount to be 2")
	assert.Equal(t, list.rowCount, 3, "Expect rowCount to be 3")
}

func TestIteratorUniqueItems(t *testing.T) {
	list := New()

	// add the items to the list
	for i := uint(0); i < 100; i++ {
		list.Append(i)
	}

	list.Iterate(func(index uint, value interface{}) {
		assert.Equal(t, index, value.(uint), "Expect the index to equal the value")
	})
}

func TestIteratorItemsInBlocks(t *testing.T) {
	list := New()

	// add the items to the list
	for i := uint(0); i < 1000; i++ {
		list.Append(uint(i / 10))
	}

	list.Iterate(func(index uint, value interface{}) {
		assert.Equal(t, uint(index/10), value.(uint), "Expect the index to equal the value")
	})
}

func BenchmarkAppendMod1(b *testing.B) {
	list := New()

	for i := 0; i < b.N; i++ {
		list.Append(int(i))
	}
}

func BenchmarkAppendMod10(b *testing.B) {
	list := New()

	for i := 0; i < b.N; i++ {
		list.Append(int(i / 10))
	}
}

func BenchmarkAppendMod100(b *testing.B) {
	list := New()

	for i := 0; i < b.N; i++ {
		list.Append(int(i / 100))
	}
}

func BenchmarkAppendMod1000(b *testing.B) {
	list := New()

	for i := 0; i < b.N; i++ {
		list.Append(int(i / 1000))
	}
}

func BenchmarkAppendRandomRange2(b *testing.B) {
	rand.Seed(42)
	randRange := 2

	list := New()

	for i := 0; i < b.N; i++ {
		list.Append(rand.Intn(randRange))
	}
}

func BenchmarkAppendRandomRange4(b *testing.B) {
	rand.Seed(42)
	randRange := 4

	list := New()

	for i := 0; i < b.N; i++ {
		list.Append(rand.Intn(randRange))
	}
}

func BenchmarkAppendRandomRange8(b *testing.B) {
	rand.Seed(42)
	randRange := 8

	list := New()

	for i := 0; i < b.N; i++ {
		list.Append(rand.Intn(randRange))
	}
}

func BenchmarkAppendRandomRange16(b *testing.B) {
	rand.Seed(42)
	randRange := 16

	list := New()

	for i := 0; i < b.N; i++ {
		list.Append(rand.Intn(randRange))
	}
}
