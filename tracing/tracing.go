// Package tracing records the dbrpc requests a caller traced as OpenTelemetry server spans.
//
// htcondordb never starts a trace of its own. A dbrpc client attaches its span's context
// to a request only when that span is sampled, and the server calls the TraceRequest hook
// only for a request that carries one, so the spans recorded here are exactly the requests
// a caller chose to trace. The sampler is ParentBased(NeverSample) to keep it that way.
//
// A span carries the operation, its outcome and response size, and for a query the table,
// row counts and scan counters. It never carries the constraint, which can name private
// attributes and their values, or the peer identity: spans are exported off the host.
package tracing

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/dbrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// ServiceName is the service.name resource attribute of every span this daemon exports.
const ServiceName = "htcondordb"

// tracerName is the instrumentation scope of the dbrpc server spans.
const tracerName = "github.com/htcondor/htcondordb/tracing"

// tracesPath is the OTLP/HTTP traces path, appended to a base endpoint URL.
const tracesPath = "/v1/traces"

// Config configures the exporter. Start is only called when Endpoint is set.
type Config struct {
	// Endpoint is the OTLP/HTTP base URL of a collector, e.g. http://otel-collector:4318.
	// Spans are posted to <Endpoint>/v1/traces; a URL that already ends in /v1/traces is
	// used as given.
	Endpoint string
	// ServiceVersion is the service.version resource attribute; empty omits it.
	ServiceVersion string
	// HostName is the host.name resource attribute; empty takes os.Hostname.
	HostName string
	// OnError, when set, receives the SDK's errors (a failed export, a full queue) in place
	// of OpenTelemetry's default handler, which writes to stderr.
	OnError func(error)
}

// Provider is a running TracerProvider exporting to an OTLP/HTTP collector.
type Provider struct {
	tp *sdktrace.TracerProvider
	// TracesURL is the URL spans are posted to.
	TracesURL string
	hook      func(context.Context, dbrpc.TracedRequest) func(dbrpc.TraceResult)
}

// ResolveEndpoint resolves a configured endpoint to the URL spans are posted to. It refuses
// anything but an absolute http or https URL.
func ResolveEndpoint(endpoint string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return "", fmt.Errorf("tracing: endpoint %q: %w", endpoint, err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("tracing: endpoint %q: want an http:// or https:// URL with a host", endpoint)
	}
	if !strings.HasSuffix(u.Path, tracesPath) {
		u.Path = strings.TrimSuffix(u.Path, "/") + tracesPath
	}
	return u.String(), nil
}

// Start builds a TracerProvider that batches spans to cfg.Endpoint and installs it as the
// process's global provider. The caller must Shutdown the returned Provider on exit, which
// flushes what is still batched.
func Start(ctx context.Context, cfg Config) (*Provider, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("tracing: no endpoint")
	}
	tracesURL, err := ResolveEndpoint(cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(tracesURL))
	if err != nil {
		return nil, fmt.Errorf("tracing: OTLP exporter: %w", err)
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(resourceAttrs(cfg)...))
	if err != nil {
		_ = exp.Shutdown(ctx)
		return nil, fmt.Errorf("tracing: resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(Sampler()),
	)
	if cfg.OnError != nil {
		otel.SetErrorHandler(otel.ErrorHandlerFunc(cfg.OnError))
	}
	otel.SetTracerProvider(tp)
	return &Provider{tp: tp, TracesURL: tracesURL, hook: Hook(tp)}, nil
}

// resourceAttrs is the resource the daemon's spans are reported under.
func resourceAttrs(cfg Config) []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String("service.name", ServiceName)}
	if cfg.ServiceVersion != "" {
		attrs = append(attrs, attribute.String("service.version", cfg.ServiceVersion))
	}
	host := cfg.HostName
	if host == "" {
		host, _ = os.Hostname()
	}
	if host != "" {
		attrs = append(attrs, attribute.String("host.name", host))
	}
	return attrs
}

// Sampler records a span only when its remote parent was sampled: the daemon never roots a
// trace of its own.
func Sampler() sdktrace.Sampler { return sdktrace.ParentBased(sdktrace.NeverSample()) }

// TraceRequest is the dbrpc.ServeOptions.TraceRequest hook for this provider.
func (p *Provider) TraceRequest() func(context.Context, dbrpc.TracedRequest) func(dbrpc.TraceResult) {
	return p.hook
}

// Shutdown flushes batched spans and stops the exporter. ctx bounds how long it waits.
func (p *Provider) Shutdown(ctx context.Context) error { return p.tp.Shutdown(ctx) }

// rpcSystem is the rpc.system attribute of every dbrpc span.
var rpcSystem = attribute.String("rpc.system", "dbrpc")

// opSpan is what a span for one opcode is started with, built once per opcode name.
type opSpan struct {
	name string
	opts []trace.SpanStartOption
}

// Hook returns a dbrpc.ServeOptions.TraceRequest function that records each traced request
// as a server span from tp, named dbrpc.<Op>, under the caller's span.
//
// The span is a sibling of the handler's work rather than its parent: dbrpc does not let
// the hook replace the handler's context. Nothing in dbrpc starts spans of its own today.
func Hook(tp trace.TracerProvider) func(context.Context, dbrpc.TracedRequest) func(dbrpc.TraceResult) {
	tracer := tp.Tracer(tracerName)
	var ops sync.Map // op name -> *opSpan
	return func(ctx context.Context, req dbrpc.TracedRequest) func(dbrpc.TraceResult) {
		// dbrpc clients send only sampled contexts; another client could send an unsampled
		// one, which the sampler would drop anyway. Skip building a span for it.
		if req.Trace.Flags&byte(trace.FlagsSampled) == 0 {
			return nil
		}
		s, ok := ops.Load(req.Op)
		if !ok {
			s, _ = ops.LoadOrStore(req.Op, &opSpan{
				name: "dbrpc." + req.Op,
				opts: []trace.SpanStartOption{
					trace.WithSpanKind(trace.SpanKindServer),
					trace.WithAttributes(rpcSystem, attribute.String("rpc.method", req.Op)),
				},
			})
		}
		op := s.(*opSpan)
		parent := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    trace.TraceID(req.Trace.TraceID),
			SpanID:     trace.SpanID(req.Trace.SpanID),
			TraceFlags: trace.TraceFlags(req.Trace.Flags),
			Remote:     true,
		})
		_, span := tracer.Start(trace.ContextWithRemoteSpanContext(ctx, parent), op.name, op.opts...)
		return func(res dbrpc.TraceResult) { finishSpan(span, res) }
	}
}

// finishSpan records what the request did on span and ends it.
//
// Error and bad-request outcomes set the span's status, described by the status name alone,
// never an error message: a message can quote the request. A missing key and a commit
// conflict are answers, so they are reported as dbrpc.status only.
//
// Scan and column counters are zero when the query path did not measure them, so a zero
// block is left out rather than reported as a scan that touched nothing.
func finishSpan(span trace.Span, res dbrpc.TraceResult) {
	if !span.IsRecording() {
		span.End()
		return
	}
	status := res.Status.String()
	switch res.Status {
	case dbrpc.TraceError, dbrpc.TraceBadRequest:
		span.SetStatus(codes.Error, status)
	}
	attrs := make([]attribute.KeyValue, 0, 20)
	attrs = append(attrs,
		attribute.String("dbrpc.status", status),
		attribute.Int("dbrpc.response.frames", res.Frames),
		attribute.Int64("dbrpc.response.bytes", res.Bytes),
		attribute.Float64("dbrpc.write_blocked_ms", float64(res.WriteBlocked)/float64(time.Millisecond)),
	)
	if res.HasQuery {
		q := res.Query
		attrs = append(attrs,
			attribute.String("db.collection.name", q.Table),
			attribute.Int("dbrpc.query.rows", q.Rows),
			attribute.Int("dbrpc.query.limit", q.Limit),
			attribute.Int("dbrpc.query.projection", q.Projection),
		)
		if sc := q.Scan; sc.SegmentsTotal > 0 {
			attrs = append(attrs,
				attribute.Int("dbrpc.scan.segments_total", sc.SegmentsTotal),
				attribute.Int("dbrpc.scan.segments_pruned", sc.SegmentsPruned),
				attribute.Int("dbrpc.scan.segments_scanned", sc.SegmentsScanned),
				attribute.Int("dbrpc.scan.segments_indexed", sc.SegmentsIndexed),
				attribute.Int("dbrpc.scan.records_visited", sc.RecordsVisited),
				attribute.Int("dbrpc.scan.records_reassembled", sc.RecordsReassembled),
				attribute.Int("dbrpc.scan.records_column_decided", sc.RecordsColumnDecided),
				attribute.Int("dbrpc.scan.rows_matched", sc.RowsMatched),
			)
		}
		if cs := q.Columns; cs.Rows > 0 {
			attrs = append(attrs,
				attribute.Int("dbrpc.columns.vector_blocks", cs.VectorBlocks),
				attribute.Int("dbrpc.columns.scope_blocks", cs.ScopeBlocks),
				attribute.Int("dbrpc.columns.pruned_blocks", cs.PrunedBlocks),
				attribute.Int("dbrpc.columns.row_windows", cs.RowWindows),
			)
		}
	}
	span.SetAttributes(attrs...)
	span.End()
}
