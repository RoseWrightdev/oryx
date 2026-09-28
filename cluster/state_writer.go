package cluster

import (
	"bytes"

	pb "github.com/rosewrightdev/oryx/api"
	"github.com/rosewrightdev/oryx/core"
	"github.com/rosewrightdev/oryx/kv"
)

// StateWriter defines the network boundary interface for applying incoming
// protobuf replication and gateway mutations to the local storage state.
type StateWriter interface {
	ApplySet(req *pb.SetRequest) error
	ApplyDelete(req *pb.DeleteRequest) error
}

// StorageStateWriter adapts core.Engine to the network StateWriter interface.
type StorageStateWriter struct {
	eng core.Engine
}

// NewStorageStateWriter creates a new StorageStateWriter adapter.
func NewStorageStateWriter(eng core.Engine) *StorageStateWriter {
	return &StorageStateWriter{eng: eng}
}

// ApplySet translates a network protobuf SetRequest into a native storage Set mutation.
func (w *StorageStateWriter) ApplySet(req *pb.SetRequest) error {
	return w.eng.Put(req.Key, kv.Value{
		Data:      bytes.Clone(req.Value),
		Timestamp: req.Timestamp,
		NodeID:    req.NodeId,
		Tombstone: false,
	})
}

// ApplyDelete translates a network protobuf DeleteRequest into a native storage Delete mutation.
func (w *StorageStateWriter) ApplyDelete(req *pb.DeleteRequest) error {
	return w.eng.Put(req.Key, kv.Value{
		Timestamp: req.Timestamp,
		NodeID:    req.NodeId,
		Tombstone: true,
	})
}

// Exists reports whether the key holds a live value in local storage.
func (w *StorageStateWriter) Exists(key kv.Key) bool {
	return w.eng.Exists(key)
}
