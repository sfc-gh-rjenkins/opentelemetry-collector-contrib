package foundationdbreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/foundationdbreceiver"

import (
	"context"
	"io"

	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/pdata"
	"go.opentelemetry.io/collector/obsreport"
)

type fdbTraceHandler interface {
	Handle(data []byte) error
}

type openTracingHandler struct {
	consumer consumer.Traces
	obsrecv  *obsreport.Receiver
}

func (h *openTracingHandler) Handle(data []byte) error {
	ctx := context.Background()
	h.obsrecv.StartTracesOp(ctx)
	traces := pdata.NewTraces()
	var trace OpenTracing
	err := msgpack.Unmarshal(data, &trace)
	if err != nil {
		if err != io.EOF {
			h.obsrecv.EndTracesOp(ctx, OPENTRACING, 0, err)
			return err
		}
	}
	span := traces.ResourceSpans().AppendEmpty().InstrumentationLibrarySpans().AppendEmpty().Spans().AppendEmpty()
	trace.getSpan(&span)
    err = h.consumer.ConsumeTraces(context.Background(), traces)
	h.obsrecv.EndTracesOp(ctx, OPENTRACING, traces.SpanCount(), err)
    return err
}

type openTelemetryHandler struct {
	consumer consumer.Traces
	obsrecv  *obsreport.Receiver
}

func (h *openTelemetryHandler) Handle(data []byte) error {
	ctx := context.Background()
	traces := pdata.NewTraces()
	var trace Trace
	err := msgpack.Unmarshal(data, &trace)
	if err != nil {
		if err != io.EOF {
			h.obsrecv.EndTracesOp(ctx, OPENTELEMETRY, 0, err)
			return err
		}
	}
	span := traces.ResourceSpans().AppendEmpty().InstrumentationLibrarySpans().AppendEmpty().Spans().AppendEmpty()
	trace.getSpan(&span)
	err = h.consumer.ConsumeTraces(context.Background(), traces)
    h.obsrecv.EndTracesOp(ctx, OPENTELEMETRY, traces.SpanCount(), err)
    return err
}
