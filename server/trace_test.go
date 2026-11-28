package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/dbrpc"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/htcondor/htcondordb/tracing"
)

var (
	callerTrace = trace.TraceID{0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36}
	callerSpan  = trace.SpanID{0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7}
)

// newTracedService builds a service whose connections record traced requests into rec,
// through the same tracing.Hook and sampler the daemon installs.
func newTracedService(t *testing.T, rec *tracetest.SpanRecorder) *Service {
	t.Helper()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec), sdktrace.WithSampler(tracing.Sampler()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	svc, err := New(Config{
		Authorize:          func(_, _, _ string) bool { return true },
		DisableMaintenance: true,
		TraceRequest:       tracing.Hook(tp),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

// dialTraced serves one in-memory connection with the options handleSession builds, and
// returns a client that knows the server's advertisement (so it may send a trace context)
// and forwards the OpenTelemetry span context of each call's ctx.
func dialTraced(t *testing.T, svc *Service) *dbrpc.Client {
	t.Helper()
	cp, sp := net.Pipe()
	go func() { _ = svc.RPC().ServeConnOpts(dbrpc.NewStreamConn(sp), svc.ServeOptions(LevelRead, "")) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp),
		dbrpc.WithPeerSessionAttrs(svc.RPC().SessionAttrs()),
		dbrpc.WithTraceContext(func(ctx context.Context) (dbrpc.TraceContext, bool) {
			sc := trace.SpanContextFromContext(ctx)
			if !sc.IsValid() {
				return dbrpc.TraceContext{}, false
			}
			return dbrpc.TraceContext{TraceID: sc.TraceID(), SpanID: sc.SpanID(), Flags: byte(sc.TraceFlags())}, true
		}))
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func callerContext(flags trace.TraceFlags) context.Context {
	return trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: callerTrace, SpanID: callerSpan, TraceFlags: flags, Remote: true,
	}))
}

// waitEnded waits for n ended spans. The server ends a span after writing the reply, so
// the client's call can return first.
func waitEnded(t *testing.T, rec *tracetest.SpanRecorder, n int) []sdktrace.ReadOnlySpan {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if ended := rec.Ended(); len(ended) >= n || time.Now().After(deadline) {
			return ended
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestTraceRequestRecordsServerSpan is the contract of the hook: a request whose caller
// sampled its span is recorded as one server span, a child of the caller's span in the
// caller's trace, carrying the RPC system and method and nothing about the peer.
func TestTraceRequestRecordsServerSpan(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	svc := newTracedService(t, rec)
	c := dialTraced(t, svc)

	ctx, cancel := context.WithTimeout(callerContext(trace.FlagsSampled), 10*time.Second)
	defer cancel()
	if _, err := c.Tables(ctx); err != nil {
		t.Fatal(err)
	}

	ended := waitEnded(t, rec, 1)
	if len(ended) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(ended))
	}
	s := ended[0]
	if s.Name() != "dbrpc.ListTables" {
		t.Errorf("span name = %q, want dbrpc.ListTables", s.Name())
	}
	if s.SpanKind() != trace.SpanKindServer {
		t.Errorf("span kind = %v, want server", s.SpanKind())
	}
	if got := s.SpanContext().TraceID(); got != callerTrace {
		t.Errorf("trace id = %s, want the caller's %s", got, callerTrace)
	}
	if got := s.Parent().SpanID(); got != callerSpan {
		t.Errorf("parent span id = %s, want the caller's %s", got, callerSpan)
	}
	if !s.Parent().IsRemote() {
		t.Error("parent is not marked remote")
	}
	want := map[attribute.Key]string{"rpc.system": "dbrpc", "rpc.method": "ListTables"}
	got := map[attribute.Key]string{}
	for _, kv := range s.Attributes() {
		got[kv.Key] = kv.Value.AsString()
	}
	if len(got) != len(want) {
		t.Errorf("attributes = %v, want exactly %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("attribute %s = %q, want %q", k, got[k], v)
		}
	}
}

// TestTraceRequestUnsampledCallerRecordsNothing: a caller that did not sample its span
// gets no server span, and neither does an untraced call.
func TestTraceRequestUnsampledCallerRecordsNothing(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	svc := newTracedService(t, rec)
	c := dialTraced(t, svc)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.Tables(callerContext(0)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tables(ctx); err != nil {
		t.Fatal(err)
	}
	// The hook runs before the handler replies, so a span for either call above would
	// already be in Started. A sampled call last proves the recorder is wired.
	if _, err := c.Tables(callerContext(trace.FlagsSampled)); err != nil {
		t.Fatal(err)
	}
	if n := len(rec.Started()); n != 1 {
		t.Fatalf("started spans = %d, want only the sampled call's", n)
	}
	if ended := waitEnded(t, rec, 1); len(ended) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(ended))
	}

	// The hook itself declines an unsampled context that a non-dbrpc client might send.
	hook := svc.ServeOptions(LevelRead, "").TraceRequest
	if end := hook(context.Background(), dbrpc.TracedRequest{
		Op:    "ListTables",
		Trace: dbrpc.TraceContext{TraceID: callerTrace, SpanID: callerSpan},
	}); end != nil {
		t.Error("hook returned a finish function for an unsampled request")
	}
}

// TestTraceRequestOffByDefault: a service configured without tracing serves connections
// with no TraceRequest hook, so dbrpc skips it entirely.
func TestTraceRequestOffByDefault(t *testing.T) {
	svc := newGateService(t)
	for _, level := range []Level{LevelRead, LevelWrite, LevelDaemon} {
		if svc.ServeOptions(level, "").TraceRequest != nil {
			t.Errorf("%s: TraceRequest set with tracing off", level)
		}
	}
}
