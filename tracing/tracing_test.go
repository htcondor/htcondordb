package tracing

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/dbrpc"
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
// only the RPC attributes on the span.
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
	end()
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
				if k := kv.GetKey(); k != "rpc.system" && k != "rpc.method" {
					t.Errorf("unexpected span attribute %s = %v", k, kv.GetValue())
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
