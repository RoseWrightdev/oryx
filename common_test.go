package oryx

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/rosewrightdev/oryx/cluster/mesh"
	"github.com/rosewrightdev/oryx/core/clock"
	"github.com/rosewrightdev/oryx/core/evict"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/credentials/insecure"
)

var mockConfig = DatabaseConfig{
	walPath:        "test_wal_dir",
	snpPath:        "test_snapshot.gob",
	walInterval:    10 * time.Millisecond,
	snpInterval:    50 * time.Millisecond,
	walBufferSize:  4096,
	walSegments:    3,
	evt:            evict.NewLRU(evict.LRUConfig{Capacity: 100, TTL: time.Hour, ShardCount: 16}),
	gossipInterval: 50 * time.Millisecond,
	clock:          clock.NewClock(),
	meshConfig:     mesh.Config{SingleNode: true},
	creds:          insecure.NewCredentials(),
}

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
}

func cleanupEngineMocks(t *testing.T) {
	if err := os.Remove(mockConfig.snpPath); err != nil && !os.IsNotExist(err) {
		assert.Nil(t, err)
	}
	if err := os.Remove(mockConfig.snpPath + ".tmp"); err != nil && !os.IsNotExist(err) {
		assert.Nil(t, err)
	}
	if err := os.RemoveAll(mockConfig.walPath); err != nil && !os.IsNotExist(err) {
		assert.Nil(t, err)
	}
}
