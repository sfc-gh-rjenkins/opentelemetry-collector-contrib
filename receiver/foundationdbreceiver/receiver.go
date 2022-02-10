// Copyright 2022 OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package foundationdbreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/foundationdbreceiver"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"

	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
)

const typeStr = "foundationdb"

type foundationDBReceiver struct {
	config.ReceiverSettings
	server   *udpServer
	consumer consumer.Traces
}

type Config struct {
	config.ReceiverSettings
}

func (c *Config) validate() error {
	return nil
}

// NewFactory creates a factory for the StatsD receiver.
func NewFactory() component.ReceiverFactory {
	return receiverhelper.NewFactory(
		typeStr,
		createDefaultConfig,
		receiverhelper.WithTraces(createTracesReceiver),
	)
}

func createDefaultConfig() config.Receiver {
	return &Config{}
}
func createTracesReceiver(ctx context.Context,
	settings component.ReceiverCreateSettings,
	cfg config.Receiver,
	consumer consumer.Traces) (component.TracesReceiver, error) {
	c := cfg.(*Config)
	return NewFoundationDBReceiver(settings, c, consumer)
}

func NewFoundationDBReceiver(settings component.ReceiverCreateSettings, config *Config,
	consumer consumer.Traces) (component.TracesReceiver, error) {
	ts, err := NewUDPServer("localhost:8889")
	if err != nil {
		log.Fatal(err)
	}
	return &foundationDBReceiver{server: ts}, nil
}

// Start starts a UDP server that can process StatsD messages.
func (f *foundationDBReceiver) Start(ctx context.Context, host component.Host) error {
	var transferChan = make(chan string, 10)
	go func() {
		if err := f.server.ListenAndServe(f.consumer, transferChan); err != nil {
			if !errors.Is(err, net.ErrClosed) {
				host.ReportFatalError(err)
			}
		}
	}()

	return nil
}

// Shutdown stops the StatsD receiver.
func (f *foundationDBReceiver) Shutdown(context.Context) error {
	return nil
}

// UDP Server Section
type udpServer struct {
	packetConn net.PacketConn
}

// NewUDPServer creates a transport.Server using UDP as its transport.
func NewUDPServer(addr string) (*udpServer, error) {
	packetConn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return nil, err
	}

	u := udpServer{
		packetConn: packetConn,
	}
	return &u, nil
}

func (u *udpServer) ListenAndServe(nextConsumer consumer.Traces, transferChan chan<- string) error {
	buf := make([]byte, 65527) // max size for udp packet body (assuming ipv6)
	for {
		n, _, err := u.packetConn.ReadFrom(buf)
		if n > 0 {
			bufCopy := make([]byte, n)
			copy(bufCopy, buf)
			u.handlePacket(bufCopy, transferChan)
		}
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
	return u.packetConn.Close()
}

type Trace struct {
	SourceIP       string
	TraceID        uint64
	SpanID         uint64
	StartTimestamp float64
	Duration       float64
	OperationName  string
	Tags           map[string]interface{}
	ParentSpanIDs  []interface{}
}

var _ msgpack.CustomDecoder = (*Trace)(nil)

func (t *Trace) DecodeMsgpack(dec *msgpack.Decoder) error {
	_, err := dec.DecodeArrayLen()
	if err != nil {
		return err
	}

	sourceIP, err := dec.DecodeString()
	if err != nil {
		return err
	}

	traceID, err := dec.DecodeUint64()
	if err != nil {
		return err
	}

	spanID, err := dec.DecodeUint64()
	if err != nil {
		return err
	}

	startTimestamp, err := dec.DecodeFloat64()
	if err != nil {
		return err
	}

	duration, err := dec.DecodeFloat64()
	if err != nil {
		return err
	}

	operation, err := dec.DecodeString()
	if err != nil {
		return err
	}

	tags, err := dec.DecodeMap()
	if err != nil {
		return err
	}

	parentIds, err := dec.DecodeSlice()
	if err != nil {
		return err
	}

	t.SourceIP = sourceIP
	t.TraceID = traceID
	t.SpanID = spanID
	t.StartTimestamp = startTimestamp
	t.Duration = duration
	t.OperationName = operation
	t.Tags = tags
	t.ParentSpanIDs = parentIds

	return nil
}

func (u *udpServer) handlePacket(data []byte, transferChan chan<- string) {
    var trace Trace
    err := msgpack.Unmarshal(data, &trace)
    if err != nil {
      fmt.Println(err)
    }
	out, err := json.MarshalIndent(trace, "", "  ")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(out))
	fmt.Printf("\n")
}
