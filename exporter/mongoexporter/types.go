package mongoexporter

import (
	"github.com/jaegertracing/jaeger/model"
	"time"
)

type KeyValue struct {
	Key      string
	VType    int32
	VStr     string
	VBool    bool
	VInt64   int64
	VFloat64 float64
	VBinary  []byte
}

type Span struct {
	TraceID       string
	SpanID        string
	OperationName string
	Flags         model.Flags
	StartTime     time.Time
	Duration      time.Duration
	Method        string
	User          string
	Tags          []KeyValue
	Warnings      []string
}
