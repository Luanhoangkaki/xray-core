package hysteria

import (
	"io"
	"sync"
	"testing"
	"time"
)

// TestInterConnClosedRace: InterConn.closed được ĐỌC trong Write() KHÔNG dùng
// lock (conn.go:132), nhưng được GHI trong udpSessionManager.close() (conn.go:183).
// Chạy với -race để phát hiện.
func TestInterConnClosedRace(t *testing.T) {
	for i := 0; i < 50; i++ {
		m := &udpSessionManager{
			m: make(map[uint32]*InterConn),
		}
		conn := &InterConn{
			id: 1,
			ch: make(chan []byte, 4),
			write: func(p []byte) error { return nil },
			close: func() {},
		}
		m.m[1] = conn

		var wg sync.WaitGroup
		wg.Add(2)

		// goroutine 1: đọc closed trong Write()
		go func() {
			defer wg.Done()
			buf := make([]byte, 64)
			for j := 0; j < 100; j++ {
				_, _ = conn.Write(buf)
			}
		}()

		// goroutine 2: ghi closed trong close()
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond)
			m.Lock()
			m.close(conn)
			m.Unlock()
		}()

		wg.Wait()
	}
}

// TestInterConnWriteShortBuffer: Write() ghi 4 byte id vào đầu p.
// Nếu len(p) < 4 -> phải trả lỗi (không panic index out of range).
func TestInterConnWriteShortBuffer(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic khi Write() với buffer ngắn hơn 4 byte: %v", r)
		}
	}()

	conn := &InterConn{
		id: 0x01020304,
		ch: make(chan []byte, 4),
		write: func(p []byte) error { return nil },
		close: func() {},
	}

	buf := make([]byte, 2) // chỉ 2 byte
	_, err := conn.Write(buf)
	if err != io.ErrShortBuffer {
		t.Fatalf("Write() với buffer ngắn phải trả io.ErrShortBuffer, nhận %v", err)
	}
}

// TestInterConnReadAfterClose: sau khi close(), Read() phải trả io.EOF
// chứ không block vĩnh viễn.
func TestInterConnReadAfterClose(t *testing.T) {
	m := &udpSessionManager{m: make(map[uint32]*InterConn)}
	conn := &InterConn{
		id: 1,
		ch: make(chan []byte, 4),
	}
	m.m[1] = conn

	m.Lock()
	m.close(conn)
	m.Unlock()

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 16)
		_, err := conn.Read(buf)
		done <- err
	}()

	select {
	case err := <-done:
		if err != io.EOF {
			t.Errorf("Read() sau close() phải trả io.EOF, nhận %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read() block vĩnh viễn sau close()")
	}
}

// TestInterConnWriteAfterClose: Write() sau close() phải trả lỗi
// (io.ErrClosedPipe), không panic, không ghi vào channel đã đóng.
func TestInterConnWriteAfterClose(t *testing.T) {
	m := &udpSessionManager{m: make(map[uint32]*InterConn)}
	conn := &InterConn{
		id: 1,
		ch: make(chan []byte, 4),
		write: func(p []byte) error { return nil },
		close: func() {},
	}
	m.m[1] = conn

	m.Lock()
	m.close(conn)
	m.Unlock()

	buf := make([]byte, 64)
	if _, err := conn.Write(buf); err != io.ErrClosedPipe {
		t.Errorf("Write() sau close() phải trả io.ErrClosedPipe, nhận %v", err)
	}
}

// TestUDPSessionManagerCloseAfterCloseChannel: đảm bảo close() hai lần
// không panic "close of closed channel".
func TestUDPSessionManagerCloseAfterCloseChannel(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic khi close() 2 lần: %v", r)
		}
	}()

	m := &udpSessionManager{m: make(map[uint32]*InterConn)}
	conn := &InterConn{id: 1, ch: make(chan []byte, 4)}
	m.m[1] = conn

	m.Lock()
	m.close(conn)
	m.close(conn)
	m.Unlock()
}
