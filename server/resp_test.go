package server

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/rosewrightdev/oryx"
)

func TestRESPServer_GetSetDeletePing(t *testing.T) {
	eng, err := oryx.NewDatabaseBuilder().Default().
		SetWalPath(t.TempDir() + "/wal").
		SetSnpPath(t.TempDir() + "/snapshot.bin").
		SetInsecure().
		SingleNode().
		Build()
	if err != nil {
		t.Fatalf("build database: %v", err)
	}
	eng.Start()
	defer eng.Stop()

	srv := NewRESPServer(eng, "127.0.0.1:0")
	go func() {
		_ = srv.Run()
	}()
	defer srv.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Addr() != "" && srv.Addr() != "127.0.0.1:0" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	addr := srv.Addr()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial RESP server: %v", err)
	}
	defer func() { _ = conn.Close() }()

	tests := []struct {
		cmd  string
		want string
	}{
		{"*1\r\n$4\r\nPING\r\n", "+PONG\r\n"},
		{"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", "+OK\r\n"},
		{"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n", "$3\r\nbar\r\n"},
		{"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n", ":1\r\n"},
		{"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n", "$-1\r\n"},
	}

	reader := bufio.NewReader(conn)
	for _, tc := range tests {
		if _, err := conn.Write([]byte(tc.cmd)); err != nil {
			t.Fatalf("write command %q: %v", tc.cmd, err)
		}
		got, err := readRESPMessage(reader)
		if err != nil {
			t.Fatalf("read response for %q: %v", tc.cmd, err)
		}
		if got != tc.want {
			t.Fatalf("unexpected response for %q: got %q want %q", tc.cmd, got, tc.want)
		}
	}
}

func TestParseRESPFrame_Malformed(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"truncated array header", "*1\r"},
		{"invalid array length", "*-5\r\n"},
		{"missing bulk prefix", "*1\r\nfoo\r\n"},
		{"truncated bulk header", "*1\r\n$5\r"},
		{"negative bulk length", "*1\r\n$-2\r\n"},
		{"huge bulk length overflow", "*1\r\n$9223372036854775807\r\nfoo\r\n"},
		{"bulk length larger than remaining data", "*1\r\n$10\r\nfoo\r\n"},
		{"missing bulk CRLF terminator", "*1\r\n$3\r\nfoo"},
		{"bad bulk CRLF terminator", "*1\r\n$3\r\nfooxx"},
		{"truncated multi-arg frame", "*2\r\n$3\r\nGET\r\n"},
		{"empty input", ""},
	}

	buf := make([][]byte, 0, 8)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("unexpected panic on %s: %v", tc.name, r)
				}
			}()
			args, read, ok := parseRESPFrame([]byte(tc.input), buf)
			if ok {
				t.Fatalf("expected malformed input to fail, but got args=%v read=%d", args, read)
			}
		})
	}
}

func readRESPMessage(r *bufio.Reader) (string, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return "", err
	}

	switch prefix {
	case '+', ':':
		line, err := readTestRESPLine(r)
		if err != nil {
			return "", err
		}
		return string(prefix) + line + "\r\n", nil
	case '$':
		line, err := readTestRESPLine(r)
		if err != nil {
			return "", err
		}
		length, err := parseInt(line)
		if err != nil {
			return "", err
		}
		if length < 0 {
			return "$-1\r\n", nil
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		if _, err := io.ReadFull(r, make([]byte, 2)); err != nil {
			return "", err
		}
		return "$" + line + "\r\n" + string(buf) + "\r\n", nil
	case '*':
		line, err := readTestRESPLine(r)
		if err != nil {
			return "", err
		}
		count, err := parseInt(line)
		if err != nil {
			return "", err
		}
		if count == 0 {
			return "*0\r\n", nil
		}
		parts := make([]string, 0, count)
		for i := 0; i < count; i++ {
			part, err := readRESPMessage(r)
			if err != nil {
				return "", err
			}
			parts = append(parts, part)
		}
		return "*" + line + "\r\n" + joinRESP(parts), nil
	default:
		return "", io.ErrUnexpectedEOF
	}
}

func readTestRESPLine(r *bufio.Reader) (string, error) {
	var b []byte
	for {
		ch, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		if ch == '\r' {
			if next, err := r.ReadByte(); err != nil {
				return "", err
			} else if next != '\n' {
				return "", io.ErrUnexpectedEOF
			}
			return string(b), nil
		}
		b = append(b, ch)
	}
}

func parseInt(s string) (int, error) {
	return strconv.Atoi(s)
}

func joinRESP(parts []string) string {
	var out string
	for _, p := range parts {
		out += p
	}
	return out
}

func BenchmarkRESPServer_Get(b *testing.B) {
	eng, err := oryx.NewDatabaseBuilder().Default().
		SetWalPath(b.TempDir() + "/wal").
		SetSnpPath(b.TempDir() + "/snapshot.bin").
		SetInsecure().
		SingleNode().
		Build()
	if err != nil {
		b.Fatalf("build database: %v", err)
	}
	eng.Start()
	defer eng.Stop()

	_ = eng.Set("benchkey", []byte("benchvalue"))

	srv := NewRESPServer(eng, "127.0.0.1:0")
	go func() {
		_ = srv.Run()
	}()
	defer srv.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Addr() != "" && srv.Addr() != "127.0.0.1:0" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	cmd := []byte("*2\r\n$3\r\nGET\r\n$8\r\nbenchkey\r\n")
	buf := make([]byte, 256)

	b.ReportAllocs()
	for b.Loop() {
		if _, err := conn.Write(cmd); err != nil {
			b.Fatalf("write: %v", err)
		}
		if _, err := conn.Read(buf); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
}

func BenchmarkRESPServer_Set(b *testing.B) {
	eng, err := oryx.NewDatabaseBuilder().Default().
		SetWalPath(b.TempDir() + "/wal").
		SetSnpPath(b.TempDir() + "/snapshot.bin").
		SetInsecure().
		SingleNode().
		Build()
	if err != nil {
		b.Fatalf("build database: %v", err)
	}
	eng.Start()
	defer eng.Stop()

	srv := NewRESPServer(eng, "127.0.0.1:0")
	go func() {
		_ = srv.Run()
	}()
	defer srv.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Addr() != "" && srv.Addr() != "127.0.0.1:0" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	cmd := []byte("*3\r\n$3\r\nSET\r\n$8\r\nbenchkey\r\n$10\r\nbenchvalue\r\n")
	buf := make([]byte, 256)

	b.ReportAllocs()
	for b.Loop() {
		if _, err := conn.Write(cmd); err != nil {
			b.Fatalf("write: %v", err)
		}
		if _, err := conn.Read(buf); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
}

func BenchmarkRESPServer_Get_Parallel(b *testing.B) {
	eng, err := oryx.NewDatabaseBuilder().Default().
		SetWalPath(b.TempDir() + "/wal").
		SetSnpPath(b.TempDir() + "/snapshot.bin").
		SetInsecure().
		SingleNode().
		Build()
	if err != nil {
		b.Fatalf("build database: %v", err)
	}
	eng.Start()
	defer eng.Stop()

	_ = eng.Set("benchkey", []byte("benchvalue"))

	addr := fmt.Sprintf("127.0.0.1:%d", 20000+rand.Intn(10000))
	srv := NewRESPServer(eng, addr)
	go func() {
		_ = srv.Run()
	}()
	defer srv.Stop()
	cmd := []byte("*2\r\n$3\r\nGET\r\n$8\r\nbenchkey\r\n")

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		var conn net.Conn
		var err error
		for i := 0; i < 100; i++ {
			conn, err = net.Dial("tcp", addr)
			if err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		defer func() { _ = conn.Close() }()
		buf := make([]byte, 256)

		for pb.Next() {
			if _, err := conn.Write(cmd); err != nil {
				return
			}
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	})
}

func BenchmarkRESPServer_Set_Parallel(b *testing.B) {
	eng, err := oryx.NewDatabaseBuilder().Default().
		SetWalPath(b.TempDir() + "/wal").
		SetSnpPath(b.TempDir() + "/snapshot.bin").
		SetInsecure().
		SingleNode().
		Build()
	if err != nil {
		b.Fatalf("build database: %v", err)
	}
	eng.Start()
	defer eng.Stop()

	addr := fmt.Sprintf("127.0.0.1:%d", 30000+rand.Intn(10000))
	srv := NewRESPServer(eng, addr)
	go func() {
		_ = srv.Run()
	}()
	defer srv.Stop()
	cmd := []byte("*3\r\n$3\r\nSET\r\n$8\r\nbenchkey\r\n$10\r\nbenchvalue\r\n")

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		var conn net.Conn
		var err error
		for i := 0; i < 100; i++ {
			conn, err = net.Dial("tcp", addr)
			if err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		defer func() { _ = conn.Close() }()
		buf := make([]byte, 256)

		for pb.Next() {
			if _, err := conn.Write(cmd); err != nil {
				return
			}
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	})
}
