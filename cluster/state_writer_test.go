package cluster

import (
	"testing"

	pb "github.com/rosewrightdev/oryx/api"
	"github.com/rosewrightdev/oryx/core"
	"github.com/rosewrightdev/oryx/core/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStorageStateWriter(t *testing.T) (*StorageStateWriter, core.Engine) {
	eng, err := core.NewEngine(core.Config{
		DisableWal:      true,
		DisableSnapshot: true,
		Clock:           clock.NewClock(),
	})
	require.NoError(t, err)
	return NewStorageStateWriter(eng), eng
}

func TestStorageStateWriter_ApplySetCopiesValue(t *testing.T) {
	adapter, eng := newTestStorageStateWriter(t)

	buf := []byte("original")
	req := &pb.SetRequest{Key: "k", Value: buf, Timestamp: 100, NodeId: "node-1"}
	assert.NoError(t, adapter.ApplySet(req))

	// Simulate pooled request buffer reuse
	copy(buf, []byte("CORRUPTED"))

	stored, ok := eng.HM().GetRecord("k")
	assert.True(t, ok)
	assert.Equal(t, []byte("original"), stored.Data)
}

func TestStorageStateWriter_ApplySetPreservesNilAndEmpty(t *testing.T) {
	adapter, eng := newTestStorageStateWriter(t)

	assert.NoError(t, adapter.ApplySet(&pb.SetRequest{Key: "nil", Timestamp: 1, NodeId: "n"}))
	stored, ok := eng.HM().GetRecord("nil")
	assert.True(t, ok)
	assert.Nil(t, stored.Data)

	assert.NoError(t, adapter.ApplySet(&pb.SetRequest{Key: "empty", Value: []byte{}, Timestamp: 1, NodeId: "n"}))
	stored, ok = eng.HM().GetRecord("empty")
	assert.True(t, ok)
	assert.NotNil(t, stored.Data)
	assert.Empty(t, stored.Data)
}

func TestStorageStateWriter_ApplyDelete(t *testing.T) {
	adapter, _ := newTestStorageStateWriter(t)

	assert.NoError(t, adapter.ApplySet(&pb.SetRequest{Key: "user:1", Value: []byte("v"), Timestamp: 10, NodeId: "n"}))
	assert.True(t, adapter.Exists("user:1"))

	assert.NoError(t, adapter.ApplyDelete(&pb.DeleteRequest{Key: "user:1", Timestamp: 20, NodeId: "n"}))
	assert.False(t, adapter.Exists("user:1"))
}
