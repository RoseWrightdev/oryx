package oryx

import (
	"fmt"
	"sync"
	"testing"

	"github.com/rosewrightdev/oryx/cluster"
	"github.com/rosewrightdev/oryx/cluster/entropy"
	"github.com/rosewrightdev/oryx/cluster/gateway"
	"github.com/rosewrightdev/oryx/cluster/mesh"
	"github.com/rosewrightdev/oryx/core/hashmap"
	"github.com/rosewrightdev/oryx/kv"
	"github.com/stretchr/testify/assert"
)

func TestEngineBasicOps(t *testing.T) {
	defer cleanupEngineMocks(t)

	eng, err := newDatabase(mockConfig)
	assert.Nil(t, err)
	eng.Start()
	defer eng.Stop()

	// Get non-existent key
	_, ok := eng.Get(kv.Key("nonexistent"))
	assert.False(t, ok)

	// Set and Get
	key, val := "user:100", []byte("john_doe")
	err = eng.Set(key, val)
	assert.Nil(t, err)

	got, ok := eng.Get(kv.Key(key))
	assert.True(t, ok)
	assert.Equal(t, val, got)

	// Delete and Get
	_, err = eng.Delete(key)
	assert.Nil(t, err)

	_, ok = eng.Get(kv.Key(key))
	assert.False(t, ok)
}

func TestEnginePersistence(t *testing.T) {
	defer cleanupEngineMocks(t)

	eng, err := newDatabase(mockConfig)
	assert.Nil(t, err)
	eng.Start()
	key1, val1 := "persist1", []byte("value1")
	key2, val2 := "persist2", []byte("value2")
	assert.Nil(t, eng.Set(key1, val1))
	assert.Nil(t, eng.Set(key2, val2))

	coreEng := eng.(*singleNodeAdapter).Core()
	err = coreEng.Snp().Create()
	assert.Nil(t, err)

	key3, val3 := "persist3", []byte("value3")
	assert.Nil(t, eng.Set(key3, val3))

	eng.Stop()

	eng2, err := newDatabase(mockConfig)
	assert.Nil(t, err)
	eng2.Start()
	defer eng2.Stop()

	v, ok := eng2.Get(kv.Key(key1))
	assert.True(t, ok)
	assert.Equal(t, val1, v)

	v, ok = eng2.Get(kv.Key(key3))
	assert.True(t, ok)
	assert.Equal(t, val3, v)
}

func TestEngine_DeletePersistence(t *testing.T) {
	defer cleanupEngineMocks(t)
	eng, _ := newDatabase(mockConfig)
	eng.Start()

	key, val := "del-persist", []byte("data")
	assert.NoError(t, eng.Set(key, val))
	_, err := eng.Delete(key)
	assert.NoError(t, err)
	eng.Stop()

	// Recover
	eng2, _ := newDatabase(mockConfig)
	eng2.Start()
	defer eng2.Stop()

	_, ok := eng2.Get(kv.Key(key))
	assert.False(t, ok, "kv.Key should still be deleted after recovery")
}

func TestEngine_LWW(t *testing.T) {
	defer cleanupEngineMocks(t)
	e, _ := newDatabase(mockConfig)
	eng := e.(*singleNodeAdapter).Core()
	eng.Start()
	defer eng.Stop()

	key := "lww-key"
	val1 := []byte("old-value")
	val2 := []byte("new-value")

	ts1 := int64(1000)
	eng.Clock().Update(ts1)
	assert.NoError(t, eng.Set(key, val1))

	ts2 := int64(2000)
	eng.Clock().Update(ts2)
	assert.NoError(t, eng.Set(key, val2))
	got, _ := eng.Get(kv.Key(key))
	assert.Equal(t, val2, got)

	// Set with older timestamp (should be ignored)
	ts3 := int64(1500)
	// We call ApplySet directly to simulate a delayed gossip arrival
	err := eng.Put(key, kv.Value{
		Data:      []byte("delayed-old-value"),
		Timestamp: ts3,
	})
	assert.NoError(t, err)
	got, _ = eng.Get(kv.Key(key))
	assert.Equal(t, val2, got, "Older timestamp should not overwrite newer data")
}

func TestEngine_TombstoneLWW(t *testing.T) {
	defer cleanupEngineMocks(t)
	e, _ := newDatabase(mockConfig)
	eng := e.(*singleNodeAdapter).Core()
	eng.Start()
	defer eng.Stop()

	key := "tomb-key"
	val := []byte("data")

	ts1 := int64(1000)
	eng.Clock().Update(ts1)
	assert.NoError(t, eng.Set(key, val))

	ts2 := int64(2000)
	eng.Clock().Update(ts2)
	_, err := eng.Delete(key)
	assert.NoError(t, err)

	_, ok := eng.Get(kv.Key(key))
	assert.False(t, ok, "kv.Key should be deleted")

	// Late-arriving Set with older timestamp
	ts3 := int64(1500)
	err = eng.Put(key, kv.Value{
		Data:      []byte("zombie"),
		Timestamp: ts3,
	})
	assert.NoError(t, err)
	_, ok = eng.Get(kv.Key(key))
	assert.False(t, ok, "Old set should not resurrect a newer tombstone")
}

func TestEngine_SyncLogic(t *testing.T) {
	defer cleanupEngineMocks(t)
	e1, _ := newDatabase(mockConfig)
	eng1 := e1.(*singleNodeAdapter).Core()
	eng1.Start()
	defer eng1.Stop()

	e2, _ := newDatabase(mockConfig)
	eng2 := e2.(*singleNodeAdapter).Core()
	eng2.Start()
	defer eng2.Stop()

	// 1. Setup eng1 with data
	key1, val1 := "sync-1", []byte("data-1")
	assert.NoError(t, eng1.Set(key1, val1))

	// 2. eng2 is empty, it pulls from eng1
	root2 := eng2.HM().RootDigest()
	shards2 := make(map[hashmap.ShardID]hashmap.Digest)
	buckets2 := make(map[hashmap.ShardID]hashmap.ShardDigest)
	eng2.HM().FillShardDigests(shards2)
	eng2.HM().FillDigests(buckets2)

	syncer1 := entropy.NewSyncer(&entropy.SyncerConfig{
		NodeID:     mockConfig.meshConfig.NodeID,
		Writer:     cluster.NewStorageStateWriter(eng1),
		Mesh:       &mesh.NopMesh{},
		MeshConfig: &mockConfig.meshConfig,
		Hm:         eng1.HM(),
		Interval:   mockConfig.gossipInterval,
		Creds:      mockConfig.creds,
		Cc:         gateway.NewClientCache(mockConfig.creds),
	})

	sets, deletes, err := syncer1.Pull(&entropy.PullConfig{
		RequesterID: "node2",
		Root:        root2,
		Shards:      shards2,
		Buckets:     buckets2,
	})
	assert.NoError(t, err)
	assert.Len(t, sets, 1)
	assert.Len(t, deletes, 0)
	assert.Equal(t, key1, sets[0].Key)

	// 3. eng2 pushes the updates
	syncer2 := entropy.NewSyncer(&entropy.SyncerConfig{
		NodeID:     mockConfig.meshConfig.NodeID,
		Writer:     cluster.NewStorageStateWriter(eng2),
		Mesh:       &mesh.NopMesh{},
		MeshConfig: &mockConfig.meshConfig,
		Hm:         eng2.HM(),
		Interval:   mockConfig.gossipInterval,
		Creds:      mockConfig.creds,
		Cc:         gateway.NewClientCache(mockConfig.creds),
	})

	err = syncer2.Push(sets, deletes)
	assert.NoError(t, err)

	got, ok := eng2.Get(kv.Key(key1))
	assert.True(t, ok)
	assert.Equal(t, val1, got)
}

func TestEngine_Concurrency(t *testing.T) {
	defer cleanupEngineMocks(t)
	eng, _ := newDatabase(mockConfig)
	eng.Start()
	defer eng.Stop()

	const (
		goroutines = 10
		iterations = 100
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			for i := range iterations {
				key := fmt.Sprintf("k-%d-%d", id, i)
				_ = eng.Set(key, []byte("v"))
				_, _ = eng.Get(kv.Key(key))
			}
		}(g)
	}

	wg.Wait()
}

func TestDatabase_VolatileMode(t *testing.T) {
	volConfig := mockConfig
	volConfig.disableWal = true
	volConfig.disableSnapshot = true
	volConfig.walPath = "nop"
	volConfig.snpPath = "nop"

	db1, err := newDatabase(volConfig)
	assert.NoError(t, err)
	db1.Start()

	key := "vol-test-key"
	val := []byte("vol-test-val")
	assert.NoError(t, db1.Set(key, val))

	got, ok := db1.Get(kv.Key(key))
	assert.True(t, ok)
	assert.Equal(t, val, got)

	db1.Stop()

	// New DB instance in volatile mode should start clean with zero persisted data
	db2, err := newDatabase(volConfig)
	assert.NoError(t, err)
	db2.Start()
	defer db2.Stop()

	_, ok = db2.Get(kv.Key(key))
	assert.False(t, ok, "key should not persist across volatile mode restarts")
}
