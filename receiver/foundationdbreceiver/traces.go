package foundationdbreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/foundationdbreceiver"

import (
	"encoding/binary"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/collector/model/pdata"
)

type SpanContext struct {
	TraceID [16]byte
	SpanID  uint64
}

type Event struct {
	Name       string
	EventTime  float64
	Attributes map[string]interface{}
}

type Trace struct {
	ArrLen         int
	TraceID        [16]byte
	SpanID         uint64
	ParentTraceID  [16]byte
	ParentSpanID   uint64
	OperationName  string
	StartTimestamp float64
	EndTimestamp   float64
	Kind           uint8
	Status         uint8
	Links          []SpanContext
	Events         []Event
	Attributes     map[string]interface{}
}

func (t *Trace) DecodeMsgpack(dec *msgpack.Decoder) error {
	arrLen, err := dec.DecodeArrayLen()
	if err != nil {
		return err
	}

	traceFirst, err := dec.DecodeUint64()
	if err != nil {
		return err
	}

	traceSecond, err := dec.DecodeUint64()
	if err != nil {
		return err
	}

	traceId := getTraceId(traceFirst, traceSecond)

	spanID, err := dec.DecodeUint64()
	if err != nil {
		return err
	}

	parentTraceFirst, err := dec.DecodeUint64()
	if err != nil {
		return err
	}

	parentTraceSecond, err := dec.DecodeUint64()
	if err != nil {
		return err
	}

	parentTraceId := getTraceId(parentTraceFirst, parentTraceSecond)

	parentSpanID, err := dec.DecodeUint64()
	if err != nil {
		return err
	}

	operation, err := dec.DecodeString()
	if err != nil {
		return err
	}

	startTime, err := dec.DecodeFloat64()
	if err != nil {
		return err
	}

	endTime, err := dec.DecodeFloat64()
	if err != nil {
		return err
	}

	kind, err := dec.DecodeUint8()
	if err != nil {
		return err
	}

	status, err := dec.DecodeUint8()
	if err != nil {
		return err
	}

	linksLen, err := dec.DecodeArrayLen()

	var links []SpanContext
	for i := 0; i < linksLen; i++ {
		linkFirst, err := dec.DecodeUint64()
		if err != nil {
			return err
		}

		linkSecond, err := dec.DecodeUint64()
		if err != nil {
			return err
		}

		linkTraceId := getTraceId(linkFirst, linkSecond)

		spanId, err := dec.DecodeUint64()
		if err != nil {
			return err
		}

		links = append(links, SpanContext{TraceID: linkTraceId, SpanID: spanId})
	}

	eventsLen, err := dec.DecodeArrayLen()
	var events []Event
	for i := 0; i < eventsLen; i++ {
		eventName, err := dec.DecodeString()
		if err != nil {
			return err
		}
		eventTime, err := dec.DecodeFloat64()
		if err != nil {
			return err
		}

		attributes := make(map[string]interface{})
		attributesLen, err := dec.DecodeArrayLen()
		if err != nil {
			return err
		}
		for i := 0; i < attributesLen; i++ {
			key, err := dec.DecodeString()
			if err != nil {
				return err
			}
			value, err := dec.DecodeString()
			if err != nil {
				return err
			}
			attributes[key] = value
		}

		events = append(events, Event{Name: eventName, EventTime: eventTime, Attributes: attributes})
	}

	attributes, err := dec.DecodeMap()
	if err != nil {
		return err
	}

	t.ArrLen = arrLen
	t.TraceID = traceId
	t.SpanID = spanID
	t.ParentTraceID = parentTraceId
	t.ParentSpanID = parentSpanID
	t.OperationName = operation
	t.StartTimestamp = startTime
	t.EndTimestamp = endTime
	t.Status = status
	t.Kind = kind
	t.Links = links
	t.Events = events
	t.Attributes = attributes

	return nil
}

func (t *Trace) getSpan(span *pdata.Span) {
	span.SetTraceID(pdata.NewTraceID(t.TraceID))
	span.SetSpanID(pdata.NewSpanID(uint64ToBytes(t.SpanID)))
	span.SetParentSpanID(pdata.NewSpanID(uint64ToBytes(t.ParentSpanID)))
	span.SetStartTimestamp(pdata.NewTimestampFromTime(timestampFromFloat64(t.StartTimestamp)))
	span.SetEndTimestamp(pdata.NewTimestampFromTime(timestampFromFloat64(t.EndTimestamp)))
	span.SetKind(pdata.SpanKindServer)
	span.Status().SetCode(pdata.StatusCodeOk)
	span.SetName(t.OperationName)
	span.SetDroppedEventsCount(0)
	span.SetDroppedAttributesCount(0)
	span.SetDroppedLinksCount(0)

	attrs := span.Attributes()
	for k, v := range t.Attributes {
		attrs.InsertString(k, v.(string))
	}

	links := span.Links()
	for _, l := range t.Links {
		link := links.AppendEmpty()
		link.SetTraceID(pdata.NewTraceID(l.TraceID))
		link.SetSpanID(pdata.NewSpanID(uint64ToBytes(l.SpanID)))
	}

	events := span.Events()
	for _, e := range t.Events {
		event := events.AppendEmpty()
		event.SetName(e.Name)
		event.SetTimestamp(pdata.NewTimestampFromTime(timestampFromFloat64(e.EventTime)))
		attrs := event.Attributes()
		for k, v := range e.Attributes {
			attrs.InsertString(k, v.(string))
		}
	}
}

type OpenTracing struct {
	ArrLen         int
	SourceIP       string
	TraceID        uint64
	SpanID         uint64
	StartTimestamp float64
	Duration       float64
	OperationName  string
	Tags           map[string]interface{}
	ParentSpanIDs  []interface{}
}

var _ msgpack.CustomDecoder = (*OpenTracing)(nil)

func (t *OpenTracing) DecodeMsgpack(dec *msgpack.Decoder) error {
	arrLen, err := dec.DecodeArrayLen()
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

	t.ArrLen = arrLen
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

func (t *OpenTracing) EncodeMsgpack(enc *msgpack.Encoder) error {
	err := enc.EncodeArrayLen(t.ArrLen)
	if err != nil {
		return err
	}

	err = enc.EncodeString(t.SourceIP)
	if err != nil {
		return err
	}

	err = enc.EncodeUint64(t.TraceID)
	if err != nil {
		return err
	}

	err = enc.EncodeUint64(t.SpanID)
	if err != nil {
		return err
	}

	err = enc.EncodeFloat64(t.StartTimestamp)
	if err != nil {
		return err
	}

	err = enc.EncodeFloat64(t.Duration)
	if err != nil {
		return err
	}

	err = enc.EncodeString(t.OperationName)
	if err != nil {
		return err
	}

	err = enc.EncodeMap(t.Tags)
	if err != nil {
		return err
	}

	err = enc.EncodeMulti(t.ParentSpanIDs)
	if err != nil {
		return err
	}

	return nil
}

func (t *OpenTracing) getSpan(span *pdata.Span) {
	span.SetTraceID(pdata.NewTraceID(uint64ToOtelTraceId(t.TraceID)))
	span.SetSpanID(pdata.NewSpanID(uint64ToBytes(t.SpanID)))
	endTime := timestampFromFloat64(t.StartTimestamp)
	durSec, durNano := durationFromFloat64(t.Duration)
	endTime = endTime.Add(time.Second * time.Duration(durSec))
	endTime = endTime.Add(time.Nanosecond * time.Duration(durNano))
	span.SetStartTimestamp(pdata.NewTimestampFromTime(timestampFromFloat64(t.StartTimestamp)))
	span.SetEndTimestamp(pdata.NewTimestampFromTime(endTime))
	span.SetKind(pdata.SpanKindServer)
	span.Status().SetCode(pdata.StatusCodeOk)
	span.SetName(t.OperationName)
	span.SetDroppedEventsCount(0)
	span.SetDroppedAttributesCount(0)
	span.SetDroppedLinksCount(0)
	if len(t.ParentSpanIDs) > 0 {
		pId := t.ParentSpanIDs[0].(uint64)
		span.SetParentSpanID(pdata.NewSpanID(uint64ToBytes(pId)))
	}

	attrs := span.Attributes()
	attrs.InsertString("sourceIP", t.SourceIP)
	for k, v := range t.Tags {
		attrs.InsertString(k, v.(string))
	}
}

func getTraceId(first, second uint64) [16]byte {
	firstTrace := uint64ToBytes(first)
	secondTrace := uint64ToBytes(second)
	var traceId [16]byte
	for i := 0; i < 16; i++ {
		if i < 8 {
			traceId[i] = firstTrace[i]
		} else {
			traceId[i] = secondTrace[i-8]
		}
	}
	return traceId
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

func uint64ToBytes(v uint64) [8]byte {
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

func uint64ToOtelTraceId(traceID uint64) [16]byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, traceID)
	var td [16]byte
	for i, x := range b {
		td[i+8] = x
	}
	return td
}
