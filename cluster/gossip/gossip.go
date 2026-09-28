// Package gossip manages decentralized membership replication and message processing.
package gossip

import (
	"log/slog"
	"sync"

	pb "github.com/rosewrightdev/oryx/api"
	"google.golang.org/protobuf/proto"
)

// StateWriter defines the interface for applying sets and deletes to the state.
type StateWriter interface {
	ApplySet(req *pb.SetRequest) error
	ApplyDelete(req *pb.DeleteRequest) error
}

// Gossip manages the replication of messages received via gossip protocols.
type Gossip struct {
	writer         StateWriter
	walEntriesPool sync.Pool
}

// NewGossip creates a new Gossip instance that handles incoming UDP replication messages.
func NewGossip(w StateWriter) *Gossip {
	return &Gossip{
		writer: w,
		walEntriesPool: sync.Pool{
			New: func() any { return &pb.WalEntry{} },
		},
	}
}

// OnGossip processes raw incoming UDP gossip packets, unmarshals them into WAL entries, and applies the updates via the StateWriter.
func (g *Gossip) OnGossip(data []byte) {
	entry := g.walEntriesPool.Get().(*pb.WalEntry)
	defer g.walEntriesPool.Put(entry)
	entry.Reset()

	if err := proto.Unmarshal(data, entry); err != nil {
		slog.Error("Failed to unmarshal gossip message", "error", err)
		return
	}

	switch kv := entry.Entry.(type) {
	case *pb.WalEntry_Set:
		if err := g.writer.ApplySet(kv.Set); err != nil {
			slog.Error("Critical: Failed to apply gossip set", "error", err)
		}
	case *pb.WalEntry_Delete:
		if err := g.writer.ApplyDelete(kv.Delete); err != nil {
			slog.Error("Critical: Failed to apply gossip delete", "error", err)
		}
	}
	entry.Entry = nil
}
