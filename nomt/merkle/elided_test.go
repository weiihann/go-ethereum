package merkle

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestElidedChildrenNew(t *testing.T) {
	ec := NewElidedChildren()
	for i := range 64 {
		assert.False(t, ec.IsElided(uint8(i)), "child %d", i)
	}
	assert.Equal(t, uint64(0), ec.Raw())
}

func TestElidedChildrenSetAndCheck(t *testing.T) {
	ec := NewElidedChildren()

	ec.SetElide(0, true)
	assert.True(t, ec.IsElided(0))
	assert.False(t, ec.IsElided(1))

	ec.SetElide(63, true)
	assert.True(t, ec.IsElided(63))

	ec.SetElide(0, false)
	assert.False(t, ec.IsElided(0))
	assert.True(t, ec.IsElided(63))
}

func TestElidedChildrenRoundTrip(t *testing.T) {
	ec := NewElidedChildren()
	ec.SetElide(5, true)
	ec.SetElide(33, true)
	ec.SetElide(62, true)

	encoded := ec.ToBytes()
	decoded := ElidedChildrenFromBytes(encoded)

	assert.True(t, decoded.IsElided(5))
	assert.True(t, decoded.IsElided(33))
	assert.True(t, decoded.IsElided(62))
	assert.False(t, decoded.IsElided(0))
	assert.Equal(t, ec.Raw(), decoded.Raw())
}

func TestElidedChildrenFromUint64(t *testing.T) {
	ec := ElidedChildrenFromUint64(0xFF)
	for i := range 8 {
		assert.True(t, ec.IsElided(uint8(i)), "child %d", i)
	}
	assert.False(t, ec.IsElided(8))
}
