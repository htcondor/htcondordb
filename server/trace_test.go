package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/dbrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
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
func dialTraced(t *testing.T, svc *Service, level Level) *dbrpc.Client {
	t.Helper()
	cp, sp := net.Pipe()
	go func() { _ = svc.RPC().ServeConnOpts(dbrpc.NewStreamConn(sp), svc.ServeOptions(level, "")) }()
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

// spanAttrs is a span's attributes by key.
func spanAttrs(s sdktrace.ReadOnlySpan) map[attribute.Key]attribute.Value {
	m := map[attribute.Key]attribute.Value{}
	for _, kv := range s.Attributes() {
		m[kv.Key] = kv.Value
	}
	return m
}

// onlySpan runs call under a sampled caller span and returns the one server span it produced.
func onlySpan(t *testing.T, rec *tracetest.SpanRecorder, call func(ctx context.Context) error) sdktrace.ReadOnlySpan {
	t.Helper()
	before := len(rec.Ended())
	ctx, cancel := context.WithTimeout(callerContext(trace.FlagsSampled), 10*time.Second)
	defer cancel()
	_ = call(ctx)
	ended := waitEnded(t, rec, before+1)
	if len(ended) != before+1 {
		t.Fatalf("ended spans = %d, want %d", len(ended), before+1)
	}
	return ended[before]
}

func wantInt(t *testing.T, a map[attribute.Key]attribute.Value, key attribute.Key, want int64) {
	t.Helper()
	v, ok := a[key]
	if !ok {
		t.Errorf("attribute %s missing, want %d", key, want)
		return
	}
	if v.Type() != attribute.INT64 || v.AsInt64() != want {
		t.Errorf("attribute %s = %v, want %d", key, v.AsInterface(), want)
	}
}

func wantStr(t *testing.T, a map[attribute.Key]attribute.Value, key attribute.Key, want string) {
	t.Helper()
	if v, ok := a[key]; !ok || v.AsString() != want {
		t.Errorf("attribute %s = %q (present %v), want %q", key, v.AsString(), ok, want)
	}
}

// TestTraceRequestRecordsServerSpan is the contract of the hook: a request whose caller
// sampled its span is recorded as one server span, a child of the caller's span in the
// caller's trace, carrying the RPC method, its outcome and its response size, and nothing
// about the peer.
func TestTraceRequestRecordsServerSpan(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	svc := newTracedService(t, rec)
	c := dialTraced(t, svc, LevelRead)

	s := onlySpan(t, rec, func(ctx context.Context) error { _, err := c.Tables(ctx); return err })
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
	if s.Status().Code != codes.Unset {
		t.Errorf("status = %v, want unset", s.Status())
	}
	a := spanAttrs(s)
	wantStr(t, a, "rpc.system", "dbrpc")
	wantStr(t, a, "rpc.method", "ListTables")
	wantStr(t, a, "dbrpc.status", "ok")
	wantInt(t, a, "dbrpc.response.frames", 1)
	if v := a["dbrpc.response.bytes"]; v.AsInt64() <= 0 {
		t.Errorf("dbrpc.response.bytes = %v, want > 0", v.AsInterface())
	}
	if v, ok := a["dbrpc.write_blocked_ms"]; !ok || v.Type() != attribute.FLOAT64 {
		t.Errorf("dbrpc.write_blocked_ms = %v (present %v), want a float", v.AsInterface(), ok)
	}
	if _, ok := a["db.collection.name"]; ok {
		t.Error("db.collection.name set on a request that is not a query")
	}
	if len(a) != 6 {
		t.Errorf("attributes = %v, want exactly the six above", a)
	}
}

// TestTraceRequestQuerySpan: a traced query reports its table, row count, LIMIT and
// projection, and never its constraint.
func TestTraceRequestQuerySpan(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	svc := newTracedService(t, rec)
	if _, err := svc.Catalog().CreateTable("jobs"); err != nil {
		t.Fatal(err)
	}
	w := dialTraced(t, svc, LevelWrite)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, key := range []string{"1.0", "1.1", "1.2"} {
		if err := putAd(ctx, w, "jobs", key); err != nil {
			t.Fatal(err)
		}
	}

	const constraint = `Owner == "alice"`
	c := dialTraced(t, svc, LevelRead)
	var rows []string
	s := onlySpan(t, rec, func(ctx context.Context) error {
		var err error
		rows, err = c.QueryRawProject(ctx, "jobs", constraint, []string{"Owner"}, 10)
		return err
	})
	if len(rows) != 3 {
		t.Fatalf("query returned %d rows, want 3", len(rows))
	}
	if s.Status().Code != codes.Unset {
		t.Errorf("status = %v, want unset", s.Status())
	}
	a := spanAttrs(s)
	wantStr(t, a, "dbrpc.status", "ok")
	wantStr(t, a, "db.collection.name", "jobs")
	wantInt(t, a, "dbrpc.query.rows", 3)
	wantInt(t, a, "dbrpc.query.limit", 10)
	wantInt(t, a, "dbrpc.query.projection", 1)
	// Every row was streamed in a frame before the terminator.
	if v := a["dbrpc.response.frames"]; v.AsInt64() < 2 {
		t.Errorf("dbrpc.response.frames = %v, want at least a row frame and a terminator", v.AsInterface())
	}
	for k, v := range a {
		if v.Type() == attribute.STRING && v.AsString() == constraint {
			t.Errorf("attribute %s carries the constraint", k)
		}
	}
}

// TestTraceRequestErrorSpan: a request that fails sets the span's status to Error, described
// by the status name rather than the error text.
func TestTraceRequestErrorSpan(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	svc := newTracedService(t, rec)
	c := dialTraced(t, svc, LevelRead)

	s := onlySpan(t, rec, func(ctx context.Context) error {
		_, err := c.ArchiveQuery(ctx, "no_such_archive", "true", 10)
		if err == nil {
			t.Error("ArchiveQuery on a missing archive succeeded")
		}
		return err
	})
	if st := s.Status(); st.Code != codes.Error || st.Description != "error" {
		t.Errorf("status = %+v, want Error described as \"error\"", st)
	}
	wantStr(t, spanAttrs(s), "dbrpc.status", "error")
}

// TestTraceRequestUnsampledCallerRecordsNothing: a caller that did not sample its span
// gets no server span, and neither does an untraced call.
func TestTraceRequestUnsampledCallerRecordsNothing(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	svc := newTracedService(t, rec)
	c := dialTraced(t, svc, LevelRead)

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
