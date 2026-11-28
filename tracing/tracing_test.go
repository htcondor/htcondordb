package tracing

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/dbrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestResolveEndpoint(t *testing.T) {
	cases := []struct {
		in, want string
		wantErr  bool
	}{
		{in: "http://otel-collector:4318", want: "http://otel-collector:4318/v1/traces"},
		{in: "http://otel-collector:4318/", want: "http://otel-collector:4318/v1/traces"},
		{in: "https://otel.example.org/otlp", want: "https://otel.example.org/otlp/v1/traces"},
		{in: "http://otel-collector:4318/v1/traces", want: "http://otel-collector:4318/v1/traces"},
		{in: " http://otel-collector:4318 ", want: "http://otel-collector:4318/v1/traces"},
		{in: "otel-collector:4318", wantErr: true},
		{in: "grpc://otel-collector:4317", wantErr: true},
		{in: "http:///v1/traces", wantErr: true},
	}
	for _, tc := range cases {
		got, err := ResolveEndpoint(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ResolveEndpoint(%q) = %q, want an error", tc.in, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("ResolveEndpoint(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
}

// TestStartExports runs the provider Start builds against a stand-in collector: a sampled
// request is posted, on Shutdown, to <endpoint>/v1/traces under the daemon's resource, with
// no attribute naming the peer.
func TestStartExports(t *testing.T) {
	var (
		mu    sync.Mutex
		paths []string
		reqs  []*collectortrace.ExportTraceServiceRequest
	)
	coll := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req collectortrace.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &req); err != nil {
			t.Errorf("collector: %v", err)
		}
		mu.Lock()
		paths = append(paths, r.URL.Path)
		reqs = append(reqs, &req)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	defer coll.Close()

	p, err := Start(context.Background(), Config{Endpoint: coll.URL, ServiceVersion: "1.2.3", HostName: "db.example.org"})
	if err != nil {
		t.Fatal(err)
	}
	end := p.TraceRequest()(context.Background(), dbrpc.TracedRequest{
		Op:    "Query",
		Peer:  "alice@example.org",
		Trace: dbrpc.TraceContext{TraceID: [16]byte{1}, SpanID: [8]byte{2}, Flags: 1},
	})
	if end == nil {
		t.Fatal("no finish function for a sampled request")
	}
	end(dbrpc.TraceResult{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(reqs) != 1 || paths[0] != "/v1/traces" {
		t.Fatalf("collector got %d exports at %v, want 1 at /v1/traces", len(reqs), paths)
	}
	rs := reqs[0].GetResourceSpans()
	if len(rs) != 1 {
		t.Fatalf("resource spans = %d, want 1", len(rs))
	}
	res := map[string]string{}
	for _, kv := range rs[0].GetResource().GetAttributes() {
		res[kv.GetKey()] = kv.GetValue().GetStringValue()
	}
	for k, v := range map[string]string{"service.name": ServiceName, "service.version": "1.2.3", "host.name": "db.example.org"} {
		if res[k] != v {
			t.Errorf("resource %s = %q, want %q", k, res[k], v)
		}
	}
	var spans int
	for _, ss := range rs[0].GetScopeSpans() {
		for _, s := range ss.GetSpans() {
			spans++
			if s.GetName() != "dbrpc.Query" {
				t.Errorf("span name = %q, want dbrpc.Query", s.GetName())
			}
			for _, kv := range s.GetAttributes() {
				k := kv.GetKey()
				if !strings.HasPrefix(k, "rpc.") && !strings.HasPrefix(k, "dbrpc.") && k != "db.collection.name" {
					t.Errorf("unexpected span attribute %s = %v", k, kv.GetValue())
				}
				if kv.GetValue().GetStringValue() == "alice@example.org" {
					t.Errorf("span attribute %s carries the peer", k)
				}
			}
		}
	}
	if spans != 1 {
		t.Errorf("exported spans = %d, want 1", spans)
	}
}

func TestStartRefusesBadEndpoint(t *testing.T) {
	if _, err := Start(context.Background(), Config{Endpoint: "otel-collector:4318"}); err == nil {
		t.Error("Start accepted an endpoint without a scheme")
	}
	if _, err := Start(context.Background(), Config{}); err == nil {
		t.Error("Start accepted an empty endpoint")
	}
}

// TestFinishSpan covers the result mapping the server tests cannot reach on demand: each
// status, and the scan and column blocks, which are reported only when measured.
func TestFinishSpan(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec), sdktrace.WithSampler(Sampler()))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	hook := Hook(tp)
	run := func(res dbrpc.TraceResult) (sdktrace.ReadOnlySpan, map[attribute.Key]attribute.Value) {
		t.Helper()
		end := hook(context.Background(), dbrpc.TracedRequest{
			Op:    "Query",
			Trace: dbrpc.TraceContext{TraceID: [16]byte{1}, SpanID: [8]byte{2}, Flags: 1},
		})
		end(res)
		ended := rec.Ended()
		s := ended[len(ended)-1]
		a := map[attribute.Key]attribute.Value{}
		for _, kv := range s.Attributes() {
			a[kv.Key] = kv.Value
		}
		return s, a
	}

	for _, tc := range []struct {
		status dbrpc.TraceStatus
		code   codes.Code
	}{
		{dbrpc.TraceOK, codes.Unset},
		{dbrpc.TraceError, codes.Error},
		{dbrpc.TraceBadRequest, codes.Error},
		{dbrpc.TraceMissing, codes.Unset},
		{dbrpc.TraceConflict, codes.Unset},
	} {
		s, a := run(dbrpc.TraceResult{Status: tc.status, Frames: 1, Bytes: 9})
		if s.Status().Code != tc.code {
			t.Errorf("%s: status code = %v, want %v", tc.status, s.Status().Code, tc.code)
		}
		if tc.code == codes.Error && s.Status().Description != tc.status.String() {
			t.Errorf("%s: status description = %q, want the status name", tc.status, s.Status().Description)
		}
		if got := a["dbrpc.status"].AsString(); got != tc.status.String() {
			t.Errorf("%s: dbrpc.status = %q", tc.status, got)
		}
		if _, ok := a["db.collection.name"]; ok {
			t.Errorf("%s: query attributes on a result without a query", tc.status)
		}
	}

	// A query with no scan or column measurements: the blocks are left out.
	_, a := run(dbrpc.TraceResult{HasQuery: true, Query: dbrpc.TracedQuery{Table: "history", Rows: 4, Limit: 5, Projection: 2}})
	if a["db.collection.name"].AsString() != "history" || a["dbrpc.query.rows"].AsInt64() != 4 ||
		a["dbrpc.query.limit"].AsInt64() != 5 || a["dbrpc.query.projection"].AsInt64() != 2 {
		t.Errorf("query attributes = %v", a)
	}
	for k := range a {
		if strings.HasPrefix(string(k), "dbrpc.scan.") || strings.HasPrefix(string(k), "dbrpc.columns.") {
			t.Errorf("unmeasured counter %s reported", k)
		}
	}

	// Measured: every counter is reported.
	q := dbrpc.TracedQuery{Table: "history", Rows: 1}
	q.Scan.SegmentsTotal, q.Scan.SegmentsPruned, q.Scan.SegmentsScanned, q.Scan.SegmentsIndexed = 10, 6, 4, 3
	q.Scan.RecordsVisited, q.Scan.RecordsReassembled, q.Scan.RecordsColumnDecided, q.Scan.RowsMatched = 100, 7, 90, 1
	q.Columns.Rows, q.Columns.VectorBlocks, q.Columns.ScopeBlocks, q.Columns.PrunedBlocks, q.Columns.RowWindows = 1, 2, 1, 5, 1
	_, a = run(dbrpc.TraceResult{HasQuery: true, Query: q, WriteBlocked: 1500 * time.Microsecond})
	for k, want := range map[attribute.Key]int64{
		"dbrpc.scan.segments_total": 10, "dbrpc.scan.segments_pruned": 6, "dbrpc.scan.segments_scanned": 4,
		"dbrpc.scan.segments_indexed": 3, "dbrpc.scan.records_visited": 100, "dbrpc.scan.records_reassembled": 7,
		"dbrpc.scan.records_column_decided": 90, "dbrpc.scan.rows_matched": 1,
		"dbrpc.columns.vector_blocks": 2, "dbrpc.columns.scope_blocks": 1, "dbrpc.columns.pruned_blocks": 5,
		"dbrpc.columns.row_windows": 1,
	} {
		if got, ok := a[k]; !ok || got.AsInt64() != want {
			t.Errorf("%s = %v (present %v), want %d", k, got.AsInterface(), ok, want)
		}
	}
	if got := a["dbrpc.write_blocked_ms"].AsFloat64(); got != 1.5 {
		t.Errorf("dbrpc.write_blocked_ms = %v, want 1.5", got)
	}
}
