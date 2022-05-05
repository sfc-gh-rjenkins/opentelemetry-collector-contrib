package foundationdbreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/foundationdbreceiver"

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/pdata"
)

type MockTraceConsumer struct {
	verifier func(td pdata.Traces) error
	err      error
}

func (mtc *MockTraceConsumer) ConsumeTraces(ctx context.Context, td pdata.Traces) error {
	if mtc.verifier != nil {
		err := mtc.verifier(td)
		if err != nil {
			return err
		}
	}
	return mtc.err
}

func (mtc *MockTraceConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{}
}

func TestHandleOpenTracingTrace(t *testing.T) {
	trace := &OpenTracing{
		ArrLen:         1,
		SourceIP:       "192.158.0.1:4000",
		TraceID:        8793247892340890,
		SpanID:         2389203490823490,
		StartTimestamp: 1646334304.666,
		Duration:       1000,
		OperationName:  "StorageUpdate",
		Tags:           map[string]interface{}{},
		ParentSpanIDs:  []interface{}{90823908902384},
	}
	data, err := msgpack.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}

	verifyTrace := func(td pdata.Traces) error {
		assert.Equal(t, 1, td.SpanCount())
		spans := td.ResourceSpans()
		span := spans.At(0).InstrumentationLibrarySpans().At(0).Spans().At(0)
		assert.Equal(t, "00000000000000009a800b91693d1f00", span.TraceID().HexString())
		assert.Equal(t, "42dd5dc9f77c0800", span.SpanID().HexString())
		assert.Equal(t, "2022-03-03 19:05:04.665999889 +0000 UTC", span.StartTimestamp().String())
		assert.Equal(t, "2022-03-03 19:21:44.665999889 +0000 UTC", span.EndTimestamp().String())
		assert.Equal(t, pdata.SpanKind(2), span.Kind())
		assert.Equal(t, pdata.StatusCodeOk, span.Status().Code())
		assert.Equal(t, "StorageUpdate", span.Name())
		assert.Equal(t, uint32(0), span.DroppedEventsCount())
		assert.Equal(t, uint32(0), span.DroppedAttributesCount())
		assert.Equal(t, uint32(0), span.DroppedLinksCount())
		assert.Equal(t, "f0c5d3969a520000", span.ParentSpanID().HexString())
		attr, ok := span.Attributes().Get("sourceIP")
		assert.True(t, ok)
		assert.Equal(t, "192.158.0.1:4000", attr.StringVal())
		return nil
	}

	mockConsumer := MockTraceConsumer{verifier: verifyTrace}
	handler := openTracingHandler{consumer: &mockConsumer}
	err = handler.Handle(data)
	assert.NoError(t, err)
}

func TestProcessMalformed(t *testing.T) {
	handler := openTracingHandler{}
	err := handler.Handle([]byte("foo"))
	assert.Error(t, err, "expected error")
}

func BenchmarkHandleTraceNoTagsOneParent(b *testing.B) {
	trace := &OpenTracing{
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

	handler := openTracingHandler{consumer: &MockTraceConsumer{}}
	for i := 0; i < b.N; i++ {
		err := handler.Handle(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHandleTraceFiveTagsThreeParent(b *testing.B) {
	trace := &OpenTracing{
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

	handler := openTracingHandler{consumer: &MockTraceConsumer{}}
	for i := 0; i < b.N; i++ {
		err := handler.Handle(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}
