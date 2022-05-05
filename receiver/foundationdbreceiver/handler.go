package foundationdbreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/foundationdbreceiver"

import (
	"context"
	"io"

	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/pdata"
)

type fdbTraceHandler interface {
	Handle(data []byte) error
}

type openTracingHandler struct {
	consumer consumer.Traces
}

func (h *openTracingHandler) Handle(data []byte) error {
	traces := pdata.NewTraces()
	var trace OpenTracing
	err := msgpack.Unmarshal(data, &trace)
	if err != nil {
		if err != io.EOF {
			return err
		}
	}
	span := traces.ResourceSpans().AppendEmpty().InstrumentationLibrarySpans().AppendEmpty().Spans().AppendEmpty()
	trace.getSpan(&span)
	return h.consumer.ConsumeTraces(context.Background(), traces)
}

type openTelemetryHandler struct {
	consumer consumer.Traces
}

func (h *openTelemetryHandler) Handle(data []byte) error {
	traces := pdata.NewTraces()
	var trace Trace
	err := msgpack.Unmarshal(data, &trace)
	if err != nil {
		if err != io.EOF {
			return err
		}
	}
	span := traces.ResourceSpans().AppendEmpty().InstrumentationLibrarySpans().AppendEmpty().Spans().AppendEmpty()
	trace.getSpan(&span)
	return h.consumer.ConsumeTraces(context.Background(), traces)
}
