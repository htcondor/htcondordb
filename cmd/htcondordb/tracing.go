package main

import (
	"context"
	"time"

	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/htcondor/golang-htcondor/config"
	"github.com/htcondor/golang-htcondor/logging"

	"github.com/htcondor/htcondordb/tracing"
)

// tracingShutdownTimeout bounds the flush of batched spans on exit, so an unreachable
// collector cannot hold up a shutdown.
const tracingShutdownTimeout = 5 * time.Second

// startTracing reads HTCONDORDB_OTEL_ENDPOINT and, when it is set, starts an OTLP/HTTP
// exporter and returns the dbrpc TraceRequest hook and a function that flushes and stops
// the exporter. Unset, or a value it cannot use, returns a nil hook and a no-op stop:
// no TracerProvider is installed and requests are served exactly as without tracing.
func startTracing(cfg *config.Config, log *logging.Logger) (hook func(context.Context, dbrpc.TracedRequest) func(dbrpc.TraceResult), stop func()) {
	stop = func() {}
	endpoint := getStr(cfg, "HTCONDORDB_OTEL_ENDPOINT")
	if endpoint == "" {
		return nil, stop
	}
	selfVer, _ := buildIdentity()
	p, err := tracing.Start(context.Background(), tracing.Config{
		Endpoint:       endpoint,
		ServiceVersion: selfVer,
		HostName:       getStr(cfg, "FULL_HOSTNAME"),
		OnError: func(err error) {
			log.Warn(logging.DestinationGeneral, "tracing export error", "err", err.Error())
		},
	})
	if err != nil {
		log.Error(logging.DestinationGeneral, "tracing disabled: HTCONDORDB_OTEL_ENDPOINT unusable",
			"endpoint", endpoint, "err", err.Error())
		return nil, stop
	}
	log.Info(logging.DestinationGeneral, "OpenTelemetry tracing enabled for requests the caller sampled",
		"export", p.TracesURL)
	return p.TraceRequest(), func() {
		ctx, cancel := context.WithTimeout(context.Background(), tracingShutdownTimeout)
		defer cancel()
		if err := p.Shutdown(ctx); err != nil {
			log.Warn(logging.DestinationGeneral, "tracing shutdown", "err", err.Error())
		}
	}
}
