package foundationdbreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/foundationdbreceiver"

import (
	"context"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/pdata"
)

type MockTraceConsumer struct {
}

func (mtc *MockTraceConsumer) ConsumeTraces(ctx context.Context, td pdata.Traces) error {
	return nil
}

func (mtc *MockTraceConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{}
}

func BenchmarkHandleTraceNoTagsOneParent(b *testing.B) {
	trace := &Trace{
		ArrLen:         1,
		SourceIP:       "192.158.0.1:4000",
		TraceID:        8793247892340890,
		SpanID:         2389203490823490,
		StartTimestamp: 12282389238923,
		Duration:       100,
		OperationName:  "foobar",
		Tags:           map[string]interface{}{},
		ParentSpanIDs:  []interface{}{90823908902384},
	}
	data, err := msgpack.Marshal(trace)
	if err != nil {
		b.Fatal(err)
	}

	fdb := &foundationDBReceiver{consumer: &MockTraceConsumer{}}
	for i := 0; i < b.N; i++ {
		err := fdb.Handle(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHandleTraceFiveTagsThreeParent(b *testing.B) {
	trace := &Trace{
		ArrLen:         1,
		SourceIP:       "192.158.0.1:4000",
		TraceID:        8793247892340890,
		SpanID:         2389203490823490,
		StartTimestamp: 12282389238923,
		Duration:       100,
		OperationName:  "foobar",
		Tags: map[string]interface{}{
			"foo":               "a very long value should go here",
			"customerID":        "abc-555444-asx",
			"jobID":             "78989234920-234-0234908",
			"availability-zone": "us-west-2c",
			"region":            "us-west",
		},
		ParentSpanIDs: []interface{}{90823908902384, 989789796876868},
	}
	data, err := msgpack.Marshal(trace)
	if err != nil {
		b.Fatal(err)
	}

	fdb := &foundationDBReceiver{consumer: &MockTraceConsumer{}}
	for i := 0; i < b.N; i++ {
		err := fdb.Handle(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}
