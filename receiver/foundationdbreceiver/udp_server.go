package foundationdbreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/foundationdbreceiver"

import (
	"context"
	"io"
	"net" // UDP Server Section

	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/collector/consumer"
)
type udpServer struct {
	conn *net.UDPConn
}

// NewUDPServer creates a transport.Server using UDP as its transport.
func NewUDPServer(addrString string, sockerBufferSize int) (*udpServer, error) {
	addr, err := net.ResolveUDPAddr("udp", addrString)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP(addr.Network(), addr)
	if err != nil {
		return nil, err
	}

	if sockerBufferSize > 0 {
		err := conn.SetReadBuffer(sockerBufferSize)
		if err != nil {
			return nil, err
		}
	}

	return &udpServer{
		conn: conn,
	}, nil
}

func (u *udpServer) ListenAndServe(handler fdbTraceHandler, maxPacketSize int) error {
	buf := make([]byte, maxPacketSize) // max size for udp packet body (assuming ipv6)
	for {
		n, _, err := u.conn.ReadFrom(buf)
		if n > 0 {
			bufCopy := make([]byte, n)
			copy(bufCopy, buf)
            // TODO - Log and continue here?
			processingErr := handler.Handle(bufCopy)
			if processingErr != nil {
				return processingErr
			}
		}
        // TODO should we check error first?
		if err != nil {
			if netErr, ok := err.(net.Error); ok {
				if netErr.Temporary() {
					continue
				}
			}
			return err
		}
	}
}

func (u *udpServer) Close() error {
	return u.conn.Close()
}

