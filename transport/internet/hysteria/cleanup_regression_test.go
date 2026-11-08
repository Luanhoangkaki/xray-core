package hysteria

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/transport/internet/stat"
)

// quicPair dựng một cặp QUIC connection thật (server conn + client conn)
// để test udpSessionManager với đúng loại *quic.Conn mà production dùng.
func quicPair(t *testing.T) (serverConn *quic.Conn, clientConn *quic.Conn, cleanup func()) {
	t.Helper()

	c, _ := cert.MustGenerate(nil)
	certificate := [][]byte{c.Certificate}
	privateKey := common.Must2(x509.ParsePKCS8PrivateKey(c.PrivateKey))

	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: certificate,
			PrivateKey:  privateKey,
		}},
		NextProtos: []string{"h3"},
	}

	quicConf := &quic.Config{
		InitialStreamReceiveWindow:     8388608,
		MaxStreamReceiveWindow:         8388608,
		InitialConnectionReceiveWindow: 8388608 * 5 / 2,
		MaxConnectionReceiveWindow:     8388608 * 5 / 2,
		MaxIdleTimeout:                 30 * time.Second,
		MaxIncomingStreams:             1024,
		EnableDatagrams:                true,
		MaxDatagramFrameSize:           MaxDatagramFrameSize,
		AssumePeerMaxDatagramFrameSize: MaxDatagramFrameSize,
		DisablePathManager:             true,
	}

	srvPktConn := common.Must2(net.ListenPacket("udp", "127.0.0.1:0"))
	srvTr := &quic.Transport{Conn: srvPktConn}
	l := common.Must2(srvTr.Listen(tlsConf, quicConf))

	cliPktConn := common.Must2(net.ListenPacket("udp", "127.0.0.1:0"))
	cliTr := &quic.Transport{Conn: cliPktConn}

	cliConn := common.Must2(cliTr.DialEarly(context.Background(), l.Addr(),
		&tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h3"}}, quicConf))

	accepted := make(chan *quic.Conn, 1)
	go func() {
		sc, err := l.Accept(context.Background())
		if err != nil {
			accepted <- nil
			return
		}
		accepted <- sc
	}()

	serverConn = <-accepted
	if serverConn == nil {
		t.Fatal("failed to accept server quic conn")
	}

	cleanup = func() {
		_ = cliConn.CloseWithError(closeErrCodeOK, "")
		_ = serverConn.CloseWithError(closeErrCodeOK, "")
		_ = cliTr.Close()
		_ = srvTr.Close()
		_ = cliPktConn.Close()
		_ = srvPktConn.Close()
		_ = l.Close()
	}

	return serverConn, cliConn, cleanup
}

func liveHeapKB() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc / 1024
}

// TestUDPSessionManagerCleanupGoroutines kiểm tra rằng sau khi QUIC
// connection bị đóng, cả hai goroutine clean() và run() đều thoát.
func TestUDPSessionManagerCleanupGoroutines(t *testing.T) {
	const conns = 20

	baseGoroutines := runtime.NumGoroutine()
	baseHeap := liveHeapKB()

	for i := 0; i < conns; i++ {
		serverConn, _, cleanup := quicPair(t)

		m := &udpSessionManager{
			conn:           serverConn,
			m:              make(map[uint32]*InterConn),
			addConn:        func(c stat.Connection) {},
			udpIdleTimeout: 30 * time.Second,
		}

		go m.clean()
		go m.run()

		// Đóng connection => ReceiveDatagram() phải trả lỗi => run() thoát
		_ = serverConn.CloseWithError(closeErrCodeOK, "")
		cleanup()
	}

	// Cho các goroutine kịp thoát + GC thu hồi
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseGoroutines+5 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)

	afterGoroutines := runtime.NumGoroutine()
	afterHeap := liveHeapKB()

	t.Logf("goroutines: %d -> %d (delta %d)", baseGoroutines, afterGoroutines, afterGoroutines-baseGoroutines)
	t.Logf("live heap KB: %d -> %d (delta %d)", baseHeap, afterHeap, afterHeap-baseHeap)

	// Cho phép một chút nhiễu từ Go runtime / test harness
	leaked := afterGoroutines - baseGoroutines
	if leaked > conns {
		t.Errorf("goroutine leak: %d goroutines còn sót sau %d connections (delta=%d)",
			leaked, conns, leaked)
	}
}

// TestUDPSessionManagerClosedRace chủ động gây race trên m.closed:
// clean() đọc m.closed KHÔNG dùng lock, run() ghi m.closed DƯỚI lock.
// Chạy với -race để phát hiện.
func TestUDPSessionManagerClosedRace(t *testing.T) {
	for i := 0; i < 5; i++ {
		serverConn, _, cleanup := quicPair(t)

		m := &udpSessionManager{
			conn:           serverConn,
			m:              make(map[uint32]*InterConn),
			addConn:        func(c stat.Connection) {},
			udpIdleTimeout: 30 * time.Second,
		}

		go m.clean()

		// Đợi để clean() đã đọc m.closed (ít nhất 1 tick = 1s) trước khi run() ghi
		time.Sleep(1200 * time.Millisecond)

		go m.run()
		time.Sleep(100 * time.Millisecond)

		// Đóng conn => run() ghi m.closed dưới lock
		_ = serverConn.CloseWithError(closeErrCodeOK, "")
		cleanup()
		time.Sleep(200 * time.Millisecond)
	}
}

// TestUDPSessionManagerCloseTwice kiểm tra close() không đóng channel hai lần
// (panic: close of closed channel).
func TestUDPSessionManagerCloseTwice(t *testing.T) {
	m := &udpSessionManager{
		m: make(map[uint32]*InterConn),
	}
	conn := &InterConn{
		id: 1,
		ch: make(chan []byte, 4),
	}
	m.m[1] = conn

	m.Lock()
	m.close(conn)
	m.close(conn) // gọi lần 2
	m.Unlock()

	if _, ok := m.m[1]; ok {
		t.Error("conn chưa bị xóa khỏi map")
	}
}

// TestUDPSessionManagerUDPAfterClose kiểm tra udp() bị từ chối sau khi closed.
func TestUDPSessionManagerUDPAfterClose(t *testing.T) {
	m := &udpSessionManager{
		m: make(map[uint32]*InterConn),
	}
	m.closed = true
	if _, err := m.udp(); err == nil {
		t.Error("udp() phải trả lỗi khi manager đã closed")
	}
}

// TestUDPSessionManagerDeadlock: run() giữ Lock() khi gọi close(),
// còn clean() gọi Lock() ở vòng lặp timeout -> kiểm tra không deadlock.
func TestUDPSessionManagerDeadlock(t *testing.T) {
	serverConn, _, cleanup := quicPair(t)
	defer cleanup()

	m := &udpSessionManager{
		conn:           serverConn,
		m:              make(map[uint32]*InterConn),
		addConn:        func(c stat.Connection) {},
		udpIdleTimeout: 10 * time.Millisecond,
	}

	done := make(chan struct{})
	go func() {
		m.clean()
		close(done)
	}()
	go m.run()

	// Đẩy conn vào map rồi đóng conn để run() ghi closed
	time.Sleep(50 * time.Millisecond)
	_ = serverConn.CloseWithError(closeErrCodeOK, "")

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: clean() không thoát sau 10s")
	}
}

var _ = errors.Is
