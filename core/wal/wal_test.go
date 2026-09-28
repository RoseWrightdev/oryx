package wal

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/rosewrightdev/oryx/kv"
	"github.com/rosewrightdev/oryx/security"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	mockWalPath       = "test_wal_dir"
	mockWalInterval   = 100 * time.Millisecond
	mockWalBufferSize = uint32(64 * 1024)
)

func cleanupWal(t *testing.T) {
	if err := os.RemoveAll(mockWalPath); err != nil && !os.IsNotExist(err) {
		assert.Nil(t, err)
	}
}

func TestNewWal(t *testing.T) {
	_, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 1)
	assert.Nil(t, err)

	cleanupWal(t)
}

func TestPublish(t *testing.T) {
	defer cleanupWal(t)

	wal, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 1)
	assert.Nil(t, err)

	val := kv.Value{Data: []byte{32}, Timestamp: 100, NodeID: "node-1"}
	err = wal.Publish("key", security.HashFunc("key"), val)
	assert.Nil(t, err)

	replay, err := wal.Replay()
	assert.Nil(t, err)
	assert.Equal(t, []byte{32}, replay["key"].Data)
	assert.Equal(t, int64(100), replay["key"].Timestamp)
}

func TestReplay(t *testing.T) {
	defer cleanupWal(t)

	wal, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 4)
	exceptedValues := make([][]byte, 1000)
	exceptedKeys := make([]string, 1000)
	assert.Nil(t, err)

	for i := range 1000 {
		key, val := strconv.Itoa(i), []byte{byte(i)}
		exceptedValues[i] = val
		exceptedKeys[i] = key
		v := kv.Value{Data: val, Timestamp: int64(i), NodeID: "n1"}
		err = wal.Publish(key, security.HashFunc(key), v)
		assert.Nil(t, err)
	}
	replay, err := wal.Replay()
	assert.Nil(t, err, "Replay returned error")

	gotValues := make([][]byte, 0, 1000)
	gotKeys := make([]string, 0, 1000)
	for k, v := range replay {
		gotKeys = append(gotKeys, k)
		gotValues = append(gotValues, v.Data)
	}

	assert.ElementsMatch(t, exceptedValues, gotValues)
	assert.ElementsMatch(t, exceptedKeys, gotKeys)
}

func TestClear(t *testing.T) {
	defer cleanupWal(t)

	wal, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 1)
	assert.Nil(t, err)

	assert.Nil(t, wal.Clear(nil))
	content, err := os.ReadFile(mockWalPath + "/seg_00.log")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(content))
}

func TestWal_PrepareSnapshot(t *testing.T) {
	defer cleanupWal(t)

	wal, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 2)
	assert.Nil(t, err)

	// Write entries so segment files are non-empty
	for i := range 20 {
		key := strconv.Itoa(i)
		val := kv.Value{Data: []byte{byte(i)}, Timestamp: int64(i), NodeID: "n1"}
		assert.Nil(t, wal.Publish(key, security.HashFunc(key), val))
	}

	offsets, err := wal.PrepareSnapshot()
	assert.Nil(t, err)
	assert.Len(t, offsets, 2, "PrepareSnapshot should return one offset per segment")

	// Each offset should be positive (segments are non-empty)
	for i, off := range offsets {
		assert.Positive(t, off, "segment %d offset should be > 0", i)
	}
}

func TestWal_ClearWithOffsets(t *testing.T) {
	defer cleanupWal(t)

	wal, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 1)
	assert.Nil(t, err)

	// Write some entries before snapshot point
	for i := range 5 {
		key := strconv.Itoa(i)
		val := kv.Value{Data: []byte{byte(i)}, Timestamp: int64(i), NodeID: "n1"}
		assert.Nil(t, wal.Publish(key, security.HashFunc(key), val))
	}

	// Capture snapshot offsets
	offsets, err := wal.PrepareSnapshot()
	assert.Nil(t, err)

	// Write more entries AFTER the snapshot point
	postKeys := []string{"post-a", "post-b", "post-c"}
	for _, k := range postKeys {
		val := kv.Value{Data: []byte("post-snapshot"), Timestamp: 999, NodeID: "n1"}
		assert.Nil(t, wal.Publish(k, security.HashFunc(k), val))
	}

	// Clear with offsets: only data before snapshot should be removed;
	// post-snapshot entries should survive.
	assert.Nil(t, wal.Clear(offsets))

	// Replay and verify only post-snapshot entries remain
	replay, err := wal.Replay()
	assert.Nil(t, err)
	for _, k := range postKeys {
		_, ok := replay[kv.Key(k)]
		assert.True(t, ok, "post-snapshot key %q should survive Clear(offsets)", k)
	}

	// Pre-snapshot keys should be gone
	for i := range 5 {
		k := kv.Key(strconv.Itoa(i))
		_, ok := replay[k]
		assert.False(t, ok, "pre-snapshot key %q should have been cleared", k)
	}
}

// TestWal_ClearLeavesOriginalIntactOnFailure pins #62: forcing the temp-file
// create to fail must never leave the segment file truncated.
func TestWal_ClearLeavesOriginalIntactOnFailure(t *testing.T) {
	defer cleanupWal(t)

	wal, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 1)
	require.NoError(t, err)

	for i := range 5 {
		key := strconv.Itoa(i)
		val := kv.Value{Data: []byte{byte(i)}, Timestamp: int64(i), NodeID: "n1"}
		require.NoError(t, wal.Publish(key, security.HashFunc(key), val))
	}

	offsets, err := wal.PrepareSnapshot()
	require.NoError(t, err)

	postKeys := []string{"post-a", "post-b"}
	for _, k := range postKeys {
		val := kv.Value{Data: []byte("post-snapshot"), Timestamp: 999, NodeID: "n1"}
		require.NoError(t, wal.Publish(k, security.HashFunc(k), val))
	}

	segPath := mockWalPath + "/seg_00.log"
	// PrepareSnapshot flushes the buffered writer without mutating data,
	// matching the flush Clear itself performs before the injected failure.
	_, err = wal.PrepareSnapshot()
	require.NoError(t, err)
	before, err := os.ReadFile(segPath)
	require.NoError(t, err)
	require.NotEmpty(t, before)

	// Occupy the temp path with a directory so OpenFile fails before any
	// write to the original file happens.
	require.NoError(t, os.Mkdir(segPath+".clear.tmp", 0750))

	err = wal.Clear(offsets)
	assert.Error(t, err)

	after, err := os.ReadFile(segPath)
	require.NoError(t, err)
	assert.Equal(t, before, after, "segment must be byte-for-byte unchanged when Clear fails")

	require.NoError(t, os.Remove(segPath+".clear.tmp"))

	// A subsequent successful Clear still works after the failed attempt.
	require.NoError(t, wal.Clear(offsets))
	replay, err := wal.Replay()
	require.NoError(t, err)
	for _, k := range postKeys {
		_, ok := replay[kv.Key(k)]
		assert.True(t, ok, "post-snapshot key %q should survive after a retried Clear", k)
	}
}

func TestWal_ClearNilOffsets(t *testing.T) {
	defer cleanupWal(t)

	wal, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 2)
	assert.Nil(t, err)

	for i := range 10 {
		key := strconv.Itoa(i)
		val := kv.Value{Data: []byte{byte(i)}, Timestamp: int64(i), NodeID: "n1"}
		assert.Nil(t, wal.Publish(key, security.HashFunc(key), val))
	}

	// Clear(nil) should truncate all segments entirely
	assert.Nil(t, wal.Clear(nil))

	replay, err := wal.Replay()
	assert.Nil(t, err)
	assert.Empty(t, replay, "all entries should be cleared when offsets is nil")
}

func TestWal_ExtraEdgeCases(t *testing.T) {
	defer cleanupWal(t)

	// 1. NewWal directory creation failure
	tmpFile, err := os.CreateTemp("", "wal-failure-test-*")
	require.NoError(t, err)
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	_, err = NewWal(tmpFile.Name(), mockWalInterval, mockWalBufferSize, 1)
	assert.Error(t, err)

	// 2. Publish Set and Delete
	wal, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 1)
	assert.NoError(t, err)
	defer wal.Stop()

	setVal := kv.Value{Data: []byte("val"), Timestamp: 200, NodeID: "n1"}
	err = wal.Publish("direct-entry", security.HashFunc("direct-entry"), setVal)
	assert.NoError(t, err)

	delVal := kv.Value{Timestamp: 250, NodeID: "n1", Tombstone: true}
	err = wal.Publish("direct-del", security.HashFunc("direct-del"), delVal)
	assert.NoError(t, err)

	// Verify Replay on sets and deletes
	replay, err := wal.Replay()
	assert.NoError(t, err)
	assert.Equal(t, []byte("val"), replay["direct-entry"].Data)
	assert.True(t, replay["direct-del"].Tombstone)

	// 3. replaySegment decode error by writing bad bytes to the log
	wal.Stop() // stop sync so we can manually edit file safely
	segPath := mockWalPath + "/seg_00.log"

	// Corrupt the file by writing an invalid header and payload
	// #nosec G304
	f, err := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0600)
	require.NoError(t, err)
	// Write header: 4 bytes size
	_, _ = f.Write([]byte{0, 0, 0, 10}) // says payload is 10 bytes
	// Write bad payload: 10 bytes of garbage
	_, _ = f.Write([]byte("garbagedata"))
	_ = f.Close()

	// Replay should fail due to wal decode error
	walReopen, err := NewWal(mockWalPath, mockWalInterval, mockWalBufferSize, 1)
	assert.NoError(t, err)
	defer walReopen.Stop()

	_, err = walReopen.Replay()
	assert.Error(t, err)
}

func TestNopWal(t *testing.T) {
	nopWal := NewNopWal()
	require.NotNil(t, nopWal)

	// Start and Stop should be no-ops
	nopWal.Start()
	nopWal.Stop()

	// Publish should accept set and delete requests without error or disk writing
	setVal := kv.Value{Data: []byte("val1"), Timestamp: 100, NodeID: "n1"}
	assert.NoError(t, nopWal.Publish("key1", security.HashFunc("key1"), setVal))

	delVal := kv.Value{Timestamp: 101, NodeID: "n1", Tombstone: true}
	assert.NoError(t, nopWal.Publish("key1", security.HashFunc("key1"), delVal))

	// Replay should return empty map
	replay, err := nopWal.Replay()
	assert.NoError(t, err)
	assert.NotNil(t, replay)
	assert.Empty(t, replay)

	// PrepareSnapshot should return nil offsets
	offsets, err := nopWal.PrepareSnapshot()
	assert.NoError(t, err)
	assert.Nil(t, offsets)

	// Clear should succeed for nil or non-nil offsets
	assert.NoError(t, nopWal.Clear(nil))
	assert.NoError(t, nopWal.Clear([]int64{100, 200}))
}
