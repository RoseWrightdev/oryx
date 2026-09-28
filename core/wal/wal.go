// Package wal provides a segmented, thread-safe Write-Ahead Log implementation.
package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/rosewrightdev/oryx/kv"
)

const (
	opSet    byte = 1
	opDelete byte = 2
)

// Waler defines the interface for a durable write-ahead log.
type Waler interface {
	Publish(key kv.Key, hash kv.HashKey, val kv.Value) error
	Replay() (map[kv.Key]kv.Value, error)
	Clear(offsets []int64) error
	PrepareSnapshot() ([]int64, error)
	Start()
	Stop()
}

type walSegment struct {
	ctx          context.Context
	cancel       context.CancelFunc
	wrt          *bufio.Writer
	file         *os.File
	path         string
	syncInterval time.Duration
	wg           sync.WaitGroup
	mu           sync.Mutex
}

func (s *walSegment) backgroundSync() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			if s.wrt.Buffered() > 0 {
				_ = s.wrt.Flush()
				_ = s.file.Sync()
			}
			s.mu.Unlock()
		case <-s.ctx.Done():
			return
		}
	}
}

// Wal implements the durable Write-Ahead Log (WAL) partitioned into segment files.
type Wal struct {
	headerPool sync.Pool
	bufferPool sync.Pool
	segments   []*walSegment
	count      int
}

// NewWal creates a new partition-segmented WAL instance.
func NewWal(dirPath string, syncInterval time.Duration, bufferSize uint32, segmentCount int) (*Wal, error) {
	if err := os.MkdirAll(dirPath, 0750); err != nil {
		return nil, err
	}

	wal := &Wal{
		segments: make([]*walSegment, segmentCount),
		count:    segmentCount,
		headerPool: sync.Pool{
			New: func() any {
				b := make([]byte, 4)
				return &b
			},
		},
		bufferPool: sync.Pool{
			New: func() any {
				b := make([]byte, 0, 2048)
				return &b
			},
		},
	}

	for i := range segmentCount {
		path := fmt.Sprintf("%s/seg_%02d.log", dirPath, i)
		// #nosec G304
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
		if err != nil {
			return nil, err
		}

		if _, err := file.Seek(0, io.SeekEnd); err != nil {
			_ = file.Close()
			// Close all segments opened so far to avoid fd leaks (#87).
			for j := 0; j < i; j++ {
				if wal.segments[j] != nil {
					wal.segments[j].cancel()
					_ = wal.segments[j].file.Close()
				}
			}
			return nil, err
		}

		ctx, cancel := context.WithCancel(context.Background())
		seg := &walSegment{
			ctx:          ctx,
			cancel:       cancel,
			mu:           sync.Mutex{},
			syncInterval: syncInterval,
			wrt:          bufio.NewWriterSize(file, int(bufferSize)),
			file:         file,
			path:         path,
		}
		wal.segments[i] = seg
	}

	return wal, nil
}

// Start spawns background sync goroutines for all log segments.
func (w *Wal) Start() {
	for _, seg := range w.segments {
		seg.wg.Add(1)
		go seg.backgroundSync()
	}
}

// Stop flushes buffers and closes all segment files.
func (w *Wal) Stop() {
	for _, seg := range w.segments {
		// Cancel the context so backgroundSync exits its select loop.
		seg.cancel()
		// Wait for backgroundSync to finish before touching the file.
		// Without this wait, a queued ticker event fires after we close
		// the file, calling Flush/Sync on a closed descriptor (#88).
		seg.wg.Wait()
		seg.mu.Lock()
		_ = seg.wrt.Flush()
		_ = seg.file.Sync()
		_ = seg.file.Close()
		seg.mu.Unlock()
	}
}

func (w *Wal) getSegment(hash kv.HashKey) *walSegment {
	// #nosec G115
	countU := uint64(w.count)
	idx := hash % countU
	// #nosec G115
	return w.segments[int(idx)]
}

// Publish appends a write entry to the partition-segmented write-ahead log under proper segment locks.
func (w *Wal) Publish(key kv.Key, hash kv.HashKey, val kv.Value) error {
	keyBytes := []byte(key)
	nodeIDBytes := []byte(val.NodeID)
	keyLen := len(keyBytes)
	nodeIDLen := len(nodeIDBytes)

	dataLen := 0
	op := opDelete
	if !val.Tombstone {
		op = opSet
		dataLen = len(val.Data)
	}

	payloadLen := 1 + 8 + 2 + keyLen + 2 + nodeIDLen
	if op == opSet {
		payloadLen += 4 + dataLen
	}

	totalLen := 4 + payloadLen
	bufPtr := w.bufferPool.Get().(*[]byte)
	buf := *bufPtr
	if cap(buf) < totalLen {
		buf = make([]byte, totalLen)
	} else {
		buf = buf[:totalLen]
	}
	*bufPtr = buf
	defer w.bufferPool.Put(bufPtr)

	binary.BigEndian.PutUint32(buf[0:4], uint32(payloadLen))
	buf[4] = op
	binary.BigEndian.PutUint64(buf[5:13], uint64(val.Timestamp))
	binary.BigEndian.PutUint16(buf[13:15], uint16(keyLen))
	copy(buf[15:15+keyLen], keyBytes)

	offset := 15 + keyLen
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(nodeIDLen))
	offset += 2
	copy(buf[offset:offset+nodeIDLen], nodeIDBytes)
	offset += nodeIDLen

	if op == opSet {
		binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(dataLen))
		offset += 4
		copy(buf[offset:offset+dataLen], val.Data)
	}

	seg := w.getSegment(hash)
	seg.mu.Lock()
	defer seg.mu.Unlock()

	if _, err := seg.wrt.Write(buf); err != nil {
		return err
	}

	return nil
}

// Replay reads all segments in parallel to reconstruct in-memory state.
func (w *Wal) Replay() (map[kv.Key]kv.Value, error) {
	results := make(map[kv.Key]kv.Value)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once

	for i := range w.count {
		wg.Add(1)
		go func(seg *walSegment) {
			defer wg.Done()
			if err := w.replaySegment(seg, results, &mu); err != nil {
				errOnce.Do(func() { firstErr = err })
			}
		}(w.segments[i])
	}

	wg.Wait()
	return results, firstErr
}

func (w *Wal) replaySegment(seg *walSegment, results map[kv.Key]kv.Value, resultsMu *sync.Mutex) error {
	seg.mu.Lock()
	defer seg.mu.Unlock()

	err := seg.wrt.Flush()
	if err != nil {
		return err
	}
	if _, err := seg.file.Seek(0, 0); err != nil {
		return err
	}

	reader := bufio.NewReader(seg.file)
	headerPtr := w.headerPool.Get().(*[]byte)
	header := *headerPtr
	defer w.headerPool.Put(headerPtr)

	payloadPtr := w.bufferPool.Get().(*[]byte)
	payload := *payloadPtr
	defer func() {
		*payloadPtr = payload
		w.bufferPool.Put(payloadPtr)
	}()

	for {
		if _, err := io.ReadFull(reader, header); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		size := int(binary.BigEndian.Uint32(header))
		if cap(payload) < size {
			payload = make([]byte, size)
		}
		payload = payload[:size]

		if _, err := io.ReadFull(reader, payload); err != nil {
			return err
		}
		if err := w.setResults(payload, results, resultsMu); err != nil {
			return err
		}
	}

	_, err = seg.file.Seek(0, io.SeekEnd)
	return err
}

func (w *Wal) setResults(payload []byte, results map[kv.Key]kv.Value, resultsMu *sync.Mutex) error {
	if len(payload) < 13 {
		return fmt.Errorf("corrupt wal entry: payload too short (%d bytes)", len(payload))
	}

	op := payload[0]
	ts := int64(binary.BigEndian.Uint64(payload[1:9]))
	keyLen := int(binary.BigEndian.Uint16(payload[9:11]))
	if len(payload) < 11+keyLen+2 {
		return fmt.Errorf("corrupt wal entry: truncated key")
	}
	key := kv.Key(payload[11 : 11+keyLen])

	offset := 11 + keyLen
	nodeIDLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
	offset += 2
	if len(payload) < offset+nodeIDLen {
		return fmt.Errorf("corrupt wal entry: truncated nodeID")
	}
	nodeID := string(payload[offset : offset+nodeIDLen])
	offset += nodeIDLen

	resultsMu.Lock()
	existing, exists := results[key]
	if !exists || ts > existing.Timestamp {
		switch op {
		case opSet:
			if len(payload) < offset+4 {
				resultsMu.Unlock()
				return fmt.Errorf("corrupt wal entry: truncated data length")
			}
			dataLen := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
			offset += 4
			if len(payload) < offset+dataLen {
				resultsMu.Unlock()
				return fmt.Errorf("corrupt wal entry: truncated data payload")
			}
			valCopy := make([]byte, dataLen)
			copy(valCopy, payload[offset:offset+dataLen])
			results[key] = kv.Value{
				Data:      valCopy,
				Timestamp: ts,
				NodeID:    nodeID,
				Tombstone: false,
			}
		case opDelete:
			results[key] = kv.Value{
				Timestamp: ts,
				NodeID:    nodeID,
				Tombstone: true,
			}
		}
	}
	resultsMu.Unlock()
	return nil
}

// PrepareSnapshot flushes buffers and returns log offsets representing current snapshot boundary.
func (w *Wal) PrepareSnapshot() ([]int64, error) {
	offsets := make([]int64, w.count)
	for i, seg := range w.segments {
		seg.mu.Lock()
		if err := seg.wrt.Flush(); err != nil {
			seg.mu.Unlock()
			return nil, err
		}
		pos, err := seg.file.Seek(0, io.SeekEnd)
		if err != nil {
			seg.mu.Unlock()
			return nil, err
		}
		offsets[i] = pos
		seg.mu.Unlock()
	}
	return offsets, nil
}

// clearCopyChunkSize bounds Clear's peak memory for the trailing-data path
// regardless of how much was written during a long snapshot (#97).
const clearCopyChunkSize = 1 << 20

// Clear truncates log segment files up to specified offsets to free disk space.
func (w *Wal) Clear(offsets []int64) error {
	for i, seg := range w.segments {
		var offset int64
		if offsets != nil && i < len(offsets) {
			offset = offsets[i]
		}
		if err := w.clearSegment(seg, offset); err != nil {
			return err
		}
	}
	return nil
}

// clearSegment drops everything in seg up to offset. Trailing bytes survive
// via copy-then-atomic-rename, so a crash never leaves it truncated (#62).
func (w *Wal) clearSegment(seg *walSegment, offset int64) error {
	seg.mu.Lock()
	defer seg.mu.Unlock()

	if err := seg.wrt.Flush(); err != nil {
		return err
	}

	currSize, err := seg.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if offset <= 0 || currSize <= offset {
		if err := seg.file.Truncate(0); err != nil {
			return err
		}
		if _, err := seg.file.Seek(0, 0); err != nil {
			return err
		}
		seg.wrt.Reset(seg.file)
		return nil
	}

	if _, err := seg.file.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	tmpPath := seg.path + ".clear.tmp"
	// #nosec G304
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err := io.CopyBuffer(tmp, seg.file, make([]byte, clearCopyChunkSize)); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, seg.path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	// The old fd now refers to an unlinked inode; reopen to pick up the
	// renamed file.
	if err := seg.file.Close(); err != nil {
		return err
	}
	// #nosec G304
	newFile, err := os.OpenFile(seg.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	if _, err := newFile.Seek(0, io.SeekEnd); err != nil {
		_ = newFile.Close()
		return err
	}

	seg.file = newFile
	seg.wrt.Reset(seg.file)
	return nil
}

// NopWal implements a zero-disk-write WAL for volatile in-memory mode.
type NopWal struct{}

// NewNopWal creates a new zero-disk-write Waler instance.
func NewNopWal() Waler { return &NopWal{} }

func (n *NopWal) Publish(_ kv.Key, _ kv.HashKey, _ kv.Value) error { return nil }
func (n *NopWal) Replay() (map[kv.Key]kv.Value, error)             { return make(map[kv.Key]kv.Value), nil }
func (n *NopWal) Clear(_ []int64) error                            { return nil }
func (n *NopWal) PrepareSnapshot() ([]int64, error)                { return nil, nil }
func (n *NopWal) Start()                                           {}
func (n *NopWal) Stop()                                            {}
