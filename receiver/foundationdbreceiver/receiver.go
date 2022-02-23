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
	"encoding/binary"
	"io"
	"time"

	//	"encoding/json"
	"errors"
	"log"
	"net"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/pdata"

	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
)

type foundationDBReceiver struct {
	config   *Config
	server   *udpServer
	consumer consumer.Traces
}


// NewFactory creates a factory for the foundationDBReceiver.
func NewFactory() component.ReceiverFactory {
	return receiverhelper.NewFactory(
		typeStr,
		createDefaultConfig,
		receiverhelper.WithTraces(createTracesReceiver),
	)
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
	ts, err := NewUDPServer(config.Address, config.SocketBufferSize)
	if err != nil {
		log.Fatal(err)
	}
	return &foundationDBReceiver{server: ts, consumer: consumer, config: config}, nil
}

// Start starts a UDP server that can process FoundationDB traces.
func (f *foundationDBReceiver) Start(ctx context.Context, host component.Host) error {
	go func() {
		if err := f.server.ListenAndServe(f.consumer, f.config.MaxPacketSize); err != nil {
			if !errors.Is(err, net.ErrClosed) {
				host.ReportFatalError(err)
			}
		}
	}()
	return nil
}

// Shutdown the foundationDBReceiver receiver.
func (f *foundationDBReceiver) Shutdown(context.Context) error {
	return nil
}

// UDP Server Section
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

	u := udpServer{
		conn: conn,
	}
	return &u, nil
}

func (u *udpServer) ListenAndServe(nextConsumer consumer.Traces, maxPacketSize int) error {
	buf := make([]byte, maxPacketSize) // max size for udp packet body (assuming ipv6)
	for {
		n, _, err := u.conn.ReadFrom(buf)
		if n > 0 {
			bufCopy := make([]byte, n)
			copy(bufCopy, buf)
			processingErr := u.handlePacket(bufCopy, nextConsumer)
			if processingErr != nil {
				return processingErr
			}
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
	return u.conn.Close()
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

func getTraces(trace *Trace) pdata.Traces {
	traces := pdata.NewTraces()
	curSpans := traces.ResourceSpans().AppendEmpty().InstrumentationLibrarySpans().AppendEmpty().Spans()
	span := curSpans.AppendEmpty()
	span.SetTraceID(pdata.NewTraceID(convertTraceId(trace.TraceID)))
	span.SetSpanID(pdata.NewSpanID(convertSpanId(trace.SpanID)))
	endTime := timestampFromFloat64(trace.StartTimestamp)
	durSec, durNano := durationFromFloat64(trace.Duration)
	endTime = endTime.Add(time.Second * time.Duration(durSec))
	endTime = endTime.Add(time.Nanosecond * time.Duration(durNano))
	span.SetStartTimestamp(pdata.NewTimestampFromTime(timestampFromFloat64(trace.StartTimestamp)))
	span.SetEndTimestamp(pdata.NewTimestampFromTime(endTime))
	span.SetKind(pdata.SpanKindServer)
	span.Status().SetCode(pdata.StatusCodeOk)
	span.Status().SetMessage("test-message")
	span.SetName(trace.OperationName)
	span.SetDroppedEventsCount(0)
	span.SetDroppedAttributesCount(0)
	span.SetDroppedLinksCount(0)
	if len(trace.ParentSpanIDs) > 0 {
		pId := trace.ParentSpanIDs[0].(uint64)
		span.SetParentSpanID(pdata.NewSpanID(convertSpanId(pId)))
	}

	attrs := span.Attributes()
	attrs.InsertString("sourceIP", trace.SourceIP)
	for k, v := range trace.Tags {
		attrs.InsertString(k, v.(string))
	}

	return traces
}

func durationFromFloat64(ts float64) (int64, int64) {
	secs := int64(ts)
	nsecs := int64((ts - float64(secs)) * 1e9)
	return secs, nsecs
}

func timestampFromFloat64(ts float64) time.Time {
	secs, nsecs := durationFromFloat64(ts)
	t := time.Unix(secs, nsecs)
	return t
}

func convertSpanId(v uint64) [8]byte {
	b := [8]byte{
		byte(0xff & v),
		byte(0xff & (v >> 8)),
		byte(0xff & (v >> 16)),
		byte(0xff & (v >> 24)),
		byte(0xff & (v >> 32)),
		byte(0xff & (v >> 40)),
		byte(0xff & (v >> 48)),
		byte(0xff & (v >> 56))}
	return b
}

func convertTraceId(traceID uint64) [16]byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, traceID)
	var td [16]byte
	for i, x := range b {
		td[i+8] = x
	}
	return td
}

func (u *udpServer) handlePacket(data []byte, consumer consumer.Traces) error {
	var trace Trace
	err := msgpack.Unmarshal(data, &trace)
	if err != nil {
		if err != io.EOF {
			return err
		}
	}

	traces := getTraces(&trace)
	err = consumer.ConsumeTraces(context.Background(), traces)
	if err != nil {
		return err
	}

	return nil
	// out, err := json.MarshalIndent(trace, "", "  ")
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// //fmt.Println(string(out))
	// fmt.Printf("\n")
	//
	// var traces pdata.NewTrace
	// var curSpans pdata.SpanSlice
	// span := curSpans.AppendEmpty()
	// span.SetSpanID(pdata.NewSpanID())
	// var ts pdata.Timestamp
	// span.SetStartTimestamp()
	//
}
