// Package core provides a standalone, high-performance embedded key-value storage engine.
package core

import (
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/rosewrightdev/oryx/core/clock"
	"github.com/rosewrightdev/oryx/core/evict"
	"github.com/rosewrightdev/oryx/core/hashmap"
	"github.com/rosewrightdev/oryx/core/snap"
	"github.com/rosewrightdev/oryx/core/wal"
	"github.com/rosewrightdev/oryx/kv"
	"github.com/rosewrightdev/oryx/security"
)

// Engine defines the standalone storage engine interface.
type Engine interface {
	Get(key kv.Key) ([]byte, bool)
	Set(key kv.Key, value []byte) error
	Put(key kv.Key, val kv.Value) error
	Delete(key kv.Key) (bool, error)
	Exists(key kv.Key) bool
	Start()
	Stop()
	HM() *hashmap.ShardedMap
	Wal() wal.Waler
	Clock() clock.Clocker
	Snp() *snap.Snapshotter
	Evt() evict.Evictor
	Evict(key kv.Key, reason evict.Reason) error
	Occupancy() float64
}

// Config specifies initialization parameters for the core storage engine.
type Config struct {
	Evt             evict.Evictor
	Clock           clock.Clocker
	WalPath         string
	SnpPath         string
	WalInterval     time.Duration
	SnpInterval     time.Duration
	WalSegments     int
	WalBufferSize   uint32
	NodeID          kv.NodeID
	DisableWal      bool
	DisableSnapshot bool
}

type engine struct {
	clock           clock.Clocker
	wal             wal.Waler
	evt             evict.Evictor
	hm              *hashmap.ShardedMap
	snp             *snap.Snapshotter
	nodeID          kv.NodeID
	disableSnapshot bool
	startOnce       sync.Once
	stopOnce        sync.Once
}

// NewEngine creates and initializes a standalone core storage engine.
func NewEngine(config Config) (Engine, error) {
	var w wal.Waler
	var err error
	if config.DisableWal {
		w = wal.NewNopWal()
	} else {
		w, err = wal.NewWal(config.WalPath, config.WalInterval, config.WalBufferSize, config.WalSegments)
		if err != nil {
			return nil, err
		}
	}

	eng := &engine{
		hm:              hashmap.NewShardedMap(),
		wal:             w,
		clock:           config.Clock,
		evt:             config.Evt,
		nodeID:          config.NodeID,
		disableSnapshot: config.DisableSnapshot,
	}

	if !config.DisableSnapshot && config.SnpPath != "" {
		if err := eng.recover(config.SnpPath); err != nil {
			slog.Error("Failed to recover database state", "error", err)
		}
	}

	stateTransferEncoder := func(enc *gob.Encoder) error {
		var encodeErr error
		var snapEntry snap.SnapshotEntry
		eng.hm.Range(func(k kv.Key, v kv.Value) bool {
			snapEntry.Key = k
			snapEntry.Data = v.Data
			snapEntry.Timestamp = v.Timestamp
			snapEntry.NodeID = kv.NodeID(v.NodeID)
			snapEntry.Tombstone = v.Tombstone

			if err := enc.Encode(&snapEntry); err != nil {
				encodeErr = fmt.Errorf("failed to encode snapshot entry: %w", err)
				return false
			}
			return true
		})
		return encodeErr
	}

	snp, err := snap.NewSnapshotter(config.SnpPath, config.SnpInterval, w, stateTransferEncoder)
	if err != nil {
		return nil, err
	}
	eng.snp = snp

	if eng.evt != nil {
		eng.evt.SetEvictCallback(eng.Evict)
	}

	return eng, nil
}

func (eng *engine) Start() {
	eng.startOnce.Do(func() {
		if eng.snp != nil && !eng.disableSnapshot {
			eng.snp.Start()
		}
		if eng.wal != nil {
			eng.wal.Start()
		}
		if eng.evt != nil {
			eng.evt.Start()
		}
	})
}

func (eng *engine) Stop() {
	eng.stopOnce.Do(func() {
		if eng.snp != nil && !eng.disableSnapshot {
			eng.snp.Stop()
		}
		if eng.wal != nil {
			eng.wal.Stop()
		}
		if eng.evt != nil {
			eng.evt.Stop()
		}
	})
}

func (eng *engine) Get(key kv.Key) ([]byte, bool) {
	data, ok := eng.hm.Get(key)
	if ok && eng.evt != nil {
		eng.evt.Publish(key, security.HashFunc(key))
	}
	return data, ok
}

func (eng *engine) Exists(key kv.Key) bool {
	_, ok := eng.hm.Get(key)
	return ok
}

// Put stores a key-value record with LWW conflict resolution and records it in the WAL.
func (eng *engine) Put(key kv.Key, val kv.Value) error {
	eng.clock.Update(val.Timestamp)

	if !eng.hm.PutLWW(key, val) {
		return nil // Stale update ignored under LWW rules
	}

	if err := eng.wal.Publish(key, security.HashFunc(key), val); err != nil {
		return fmt.Errorf("failed to persist write to WAL: %w", err)
	}
	return nil
}

func (eng *engine) Set(key kv.Key, value []byte) error {
	if eng.evt != nil {
		eng.evt.Publish(key, security.HashFunc(key))
	}
	return eng.Put(key, kv.Value{
		Data:      value,
		Timestamp: eng.clock.Now(),
		NodeID:    string(eng.nodeID),
	})
}

func (eng *engine) Delete(key kv.Key) (bool, error) {
	if !eng.Exists(key) {
		return false, nil
	}

	if eng.evt != nil {
		eng.evt.PublishDelete(key, security.HashFunc(key))
	}

	err := eng.Put(key, kv.Value{
		Timestamp: eng.clock.Now(),
		NodeID:    string(eng.nodeID),
		Tombstone: true,
	})
	return true, err
}

func (eng *engine) Evict(key kv.Key, reason evict.Reason) error {
	ts := eng.clock.Now()
	val := kv.Value{
		Timestamp: ts,
		NodeID:    string(eng.nodeID),
		Tombstone: true,
	}
	if reason == evict.ReasonCapacity {
		eng.hm.Delete(key)
		if err := eng.wal.Publish(key, security.HashFunc(key), val); err != nil {
			return fmt.Errorf("failed to persist eviction to WAL: %w", err)
		}
		return nil
	}
	return eng.Put(key, val)
}

func (eng *engine) HM() *hashmap.ShardedMap {
	return eng.hm
}

func (eng *engine) Wal() wal.Waler {
	return eng.wal
}

func (eng *engine) Clock() clock.Clocker {
	return eng.clock
}

func (eng *engine) Snp() *snap.Snapshotter {
	return eng.snp
}

func (eng *engine) Evt() evict.Evictor {
	return eng.evt
}

func (eng *engine) Occupancy() float64 {
	if occupier, ok := eng.evt.(interface{ Occupancy() float64 }); ok {
		return occupier.Occupancy()
	}
	return 0.0
}

func (eng *engine) recover(snpPath string) error {
	if info, err := os.Stat(snpPath); err == nil && info.Size() > 0 {
		file, err := os.Open(snpPath)
		if err != nil {
			return err
		}
		defer func() {
			_ = file.Close()
		}()

		dec := gob.NewDecoder(file)
		count := 0
		for {
			var entry snap.SnapshotEntry
			if err := dec.Decode(&entry); err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			eng.hm.Put(entry.Key, kv.Value{
				Data:      entry.Data,
				Timestamp: entry.Timestamp,
				NodeID:    string(entry.NodeID),
				Tombstone: entry.Tombstone,
			})
			count++
		}
		slog.Info("Loaded state from snapshot", "path", snpPath, "keys", count)
	}

	updates, err := eng.wal.Replay()
	if err != nil {
		return err
	}
	for k, v := range updates {
		eng.hm.PutLWW(k, v)
	}
	if len(updates) > 0 {
		slog.Info("Replayed updates from WAL", "count", len(updates))
	}

	return nil
}
