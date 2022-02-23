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
	"encoding/json"
	"fmt"
	"io"
	"time"

	"errors"
	"net"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"

	"github.com/vmihailenco/msgpack/v5"
)

type foundationDBReceiver struct {
	config   *Config
	server   *udpServer
	consumer consumer.Traces
	logger   *zap.Logger
}

type fdbTraceHandler interface {
	Handle(data []byte) error
}

// Start the foundationDBReceiver.
func (f *foundationDBReceiver) Start(ctx context.Context, host component.Host) error {
	go func() {
		f.logger.Info("Starting UDP server.")
		if err := f.server.ListenAndServe(f, f.config.MaxPacketSize); err != nil {
			if !errors.Is(err, net.ErrClosed) {
				host.ReportFatalError(err)
			}
		}
	}()

	// In practice we have not seen the context being canceled and Done
	// signaled however implementing for correctness.
	go func() {
		for {
			select {
			case <-ctx.Done():
				f.logger.Info("Receiver selected Done signal.")
				f.server.Close()
			}
		}
	}()
	return nil
}

// Shutdown the foundationDBReceiver receiver.
func (f *foundationDBReceiver) Shutdown(context.Context) error {
	f.logger.Info("Trace receiver received Shutdown.")
	err := f.server.conn.Close()
	if err != nil {
		f.logger.Sugar().Debugf("Error received attempting to close server: %s", err.Error())
	}
	return err
}

// Accepts a []byte of MessagePack encoded FoundationDB traces, converts to intermediary in memory
// Trace struct format, then generates OTEL complian traces from data and forward to our trace consumer.
func (f *foundationDBReceiver) Handle(data []byte) error {
	var trace Trace
	err := msgpack.Unmarshal(data, &trace)
	if err != nil {
		if err != io.EOF {
			return err
		}
	}

	traces := getTraces(&trace)
	err = f.consumer.ConsumeTraces(context.Background(), traces)
	if err != nil {
		return err
	}

	return nil
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

func prettyPrintTrace(trace *Trace) {
	out, err := json.MarshalIndent(trace, "", "  ")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(out))
}
