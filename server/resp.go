package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/panjf2000/gnet/v2"
	"github.com/rosewrightdev/oryx"
	"github.com/rosewrightdev/oryx/kv"
)

var (
	respOK            = []byte("+OK\r\n")
	respPONG          = []byte("+PONG\r\n")
	respNil           = []byte("$-1\r\n")
	respZeroInt       = []byte(":0\r\n")
	respOneInt        = []byte(":1\r\n")
	respErrCmd        = []byte("-ERR unknown command\r\n")
	respErrArgs       = []byte("-ERR wrong number of arguments\r\n")
	respErrBadRequest = []byte("-ERR bad request\r\n")
	crlfBytes         = []byte("\r\n")

	_ = respErrBadRequest
	_ = newGnetServer
)

const (
	// maxArgs caps the number of RESP array arguments to prevent DoS memory exhaustion.
	maxArgs = 1024
	// maxInt is the maximum value of a signed integer on this platform.
	maxInt = int(^uint(0) >> 1)
)

type connState struct {
	argBuf [][]byte
	outBuf []byte
	vecBuf [][]byte
}

type RESPServer struct {
	*gnet.BuiltinEventEngine
	eng          oryx.Database
	addr         string
	stopOnce     sync.Once
	mu           sync.Mutex
	bound        string
	resolvedAddr string
	gnetEng      gnet.Engine
	hasGnetEng   bool
}

func NewRESPServer(eng oryx.Database, addr string) *RESPServer {
	return &RESPServer{
		eng:  eng,
		addr: addr,
	}
}

func newGnetServer(eng oryx.Database, addr string) *RESPServer {
	return NewRESPServer(eng, addr)
}

func (s *RESPServer) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bound != "" {
		return s.bound
	}
	return s.addr
}

func (s *RESPServer) OnBoot(eng gnet.Engine) gnet.Action {
	s.mu.Lock()
	s.bound = s.resolvedAddr
	s.gnetEng = eng
	s.hasGnetEng = true
	s.mu.Unlock()
	return gnet.None
}

func (s *RESPServer) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	c.SetContext(&connState{
		argBuf: make([][]byte, 0, 32),
		outBuf: make([]byte, 0, 65536),
		vecBuf: make([][]byte, 0, 32),
	})
	return
}

func (s *RESPServer) OnClose(c gnet.Conn, err error) gnet.Action {
	return gnet.None
}

func (s *RESPServer) OnTraffic(c gnet.Conn) gnet.Action {
	state, _ := c.Context().(*connState)
	if state == nil {
		state = &connState{
			argBuf: make([][]byte, 0, 32),
			outBuf: make([]byte, 0, 65536),
			vecBuf: make([][]byte, 0, 32),
		}
		c.SetContext(state)
	}

	data, _ := c.Next(-1)
	state.outBuf = state.outBuf[:0]
	state.vecBuf = state.vecBuf[:0]

	for len(data) > 0 {
		args, read, ok := parseRESPFrame(data, state.argBuf)
		if !ok {
			break
		}
		data = data[read:]
		prevLen := len(state.outBuf)
		state.outBuf = s.dispatchToBuffer(args, state.outBuf)
		if len(state.outBuf) > prevLen {
			state.vecBuf = append(state.vecBuf, state.outBuf[prevLen:])
		}
	}

	if len(state.vecBuf) > 0 {
		if len(state.vecBuf) == 1 {
			_, _ = c.Write(state.vecBuf[0])
		} else {
			_, _ = c.Writev(state.vecBuf)
		}
	}

	return gnet.None
}

func stringFromBytes(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func (s *RESPServer) dispatchToBuffer(args [][]byte, out []byte) []byte {
	if len(args) == 0 {
		return append(out, "-ERR empty command\r\n"...)
	}

	cmd := args[0]
	switch len(cmd) {
	case 3:
		if bytes.EqualFold(cmd, []byte("GET")) {
			if len(args) != 2 {
				return append(out, respErrArgs...)
			}
			val, ok := s.eng.Get(kv.Key(stringFromBytes(args[1])))
			if !ok {
				return append(out, respNil...)
			}
			return appendBulkBytes(out, val)
		}
		if bytes.EqualFold(cmd, []byte("SET")) {
			if len(args) != 3 {
				return append(out, respErrArgs...)
			}
			// Clone both the key and value out of the gnet socket buffer before
			// storing them. The buffer is reused by the event loop for new packets,
			// which would silently corrupt any stored slices/strings that alias it.
			key := string(args[1]) // safe heap copy
			val := make([]byte, len(args[2]))
			copy(val, args[2])
			if err := s.eng.Set(kv.Key(key), val); err != nil {
				return append(out, fmt.Sprintf("-ERR %v\r\n", err)...)
			}
			return append(out, respOK...)
		}
		if bytes.EqualFold(cmd, []byte("DEL")) {
			if len(args) < 2 {
				return append(out, respErrArgs...)
			}
			count := 0
			for _, keyBytes := range args[1:] {
				// Use string(keyBytes) for a safe heap-allocated copy so the
				// key is not aliasing the volatile gnet socket buffer.
				existed, err := s.eng.Delete(kv.Key(string(keyBytes)))
				if err != nil {
					return append(out, fmt.Sprintf("-ERR %v\r\n", err)...)
				}
				if existed {
					count++
				}
			}
			return appendInt(out, count)
		}

	case 4:
		if bytes.EqualFold(cmd, []byte("PING")) {
			if len(args) > 1 {
				return appendBulkBytes(out, args[1])
			}
			return append(out, respPONG...)
		}
		if bytes.EqualFold(cmd, []byte("ECHO")) {
			if len(args) < 2 {
				return append(out, respNil...)
			}
			return appendBulkBytes(out, args[1])
		}
		if bytes.EqualFold(cmd, []byte("QUIT")) {
			return append(out, respOK...)
		}

	case 6:
		if bytes.EqualFold(cmd, []byte("EXISTS")) {
			if len(args) != 2 {
				return append(out, respErrArgs...)
			}
			_, ok := s.eng.Get(kv.Key(args[1]))
			if ok {
				return append(out, respOneInt...)
			}
			return append(out, respZeroInt...)
		}
	}

	return append(out, respErrCmd...)
}

func appendBulkBytes(out []byte, val []byte) []byte {
	out = append(out, '$')
	out = strconv.AppendInt(out, int64(len(val)), 10)
	out = append(out, crlfBytes...)
	out = append(out, val...)
	out = append(out, crlfBytes...)
	return out
}

func appendInt(out []byte, val int) []byte {
	out = append(out, ':')
	out = strconv.AppendInt(out, int64(val), 10)
	out = append(out, crlfBytes...)
	return out
}

func parseRESPFrame(data []byte, argBuf [][]byte) ([][]byte, int, bool) {
	if len(data) == 0 {
		return nil, 0, false
	}
	if data[0] != '*' {
		return nil, 0, false
	}

	idx := bytes.IndexByte(data, '\n')
	if idx < 0 || idx < 2 || data[idx-1] != '\r' {
		return nil, 0, false
	}

	numArgs, err := parseFastInt(data[1 : idx-1])
	if err != nil || numArgs < 0 {
		return nil, 0, false
	}
	if numArgs > maxArgs {
		return nil, 0, false
	}

	read := idx + 1
	argBuf = argBuf[:0]

	for range numArgs {
		if read >= len(data) || data[read] != '$' {
			return nil, 0, false
		}
		crlfIdx := bytes.IndexByte(data[read:], '\n')
		if crlfIdx < 0 || crlfIdx < 2 || data[read+crlfIdx-1] != '\r' {
			return nil, 0, false
		}

		bulkLen, err := parseFastInt(data[read+1 : read+crlfIdx-1])
		if err != nil || bulkLen < 0 {
			return nil, 0, false
		}

		bulkStart := read + crlfIdx + 1
		if len(data)-bulkStart < 2 || bulkLen > len(data)-bulkStart-2 {
			return nil, 0, false
		}
		bulkEnd := bulkStart + bulkLen

		if data[bulkEnd] != '\r' || data[bulkEnd+1] != '\n' {
			return nil, 0, false
		}

		argBuf = append(argBuf, data[bulkStart:bulkEnd])
		read = bulkEnd + 2
	}

	return argBuf, read, true
}

func parseFastInt(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, errors.New("empty int")
	}
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, errors.New("invalid digit")
		}
		// Guard against integer overflow before accumulating.
		digit := int(c - '0')
		if n > (maxInt-digit)/10 {
			return 0, errors.New("integer overflow")
		}
		n = n*10 + digit
	}
	return n, nil
}

func (s *RESPServer) Run() error {
	addr := s.addr
	if strings.HasSuffix(addr, ":0") || addr == ":0" {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		port := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		time.Sleep(10 * time.Millisecond)
		addr = fmt.Sprintf("127.0.0.1:%d", port)
	}

	s.mu.Lock()
	s.resolvedAddr = addr
	s.mu.Unlock()

	protoAddr := "tcp://" + addr
	return gnet.Run(s, protoAddr,
		gnet.WithMulticore(true),
		gnet.WithReusePort(true),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
	)
}

func (s *RESPServer) Stop() {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		eng := s.gnetEng
		hasEng := s.hasGnetEng
		s.mu.Unlock()
		if hasEng {
			_ = eng.Stop(context.Background())
		}
	})
}
