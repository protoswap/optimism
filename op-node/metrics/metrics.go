package metrics

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	Namespace = "op_node"

	RPCServerSubsystem = "rpc_server"
	RPCClientSubsystem = "rpc_client"

	BatchMethod = "<batch>"
)

var (
	Info = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Name:      "info",
		Help:      "Pseudo-metric tracking version and config info",
	}, []string{
		"version",
	})
	Up = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Name:      "up",
		Help:      "1 if the op node has finished starting up",
	})
	RPCServerRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: RPCServerSubsystem,
		Name:      "requests_total",
		Help:      "Total requests to the RPC server",
	}, []string{
		"method",
	})
	RPCServerRequestDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace,
		Subsystem: RPCServerSubsystem,
		Name:      "request_duration_seconds",
		Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		Help:      "Histogram of RPC server request durations",
	}, []string{
		"method",
	})
	RPCClientRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: RPCClientSubsystem,
		Name:      "requests_total",
		Help:      "Total RPC requests initiated by the opnode's RPC client",
	}, []string{
		"method",
	})
	RPCClientRequestDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace,
		Subsystem: RPCClientSubsystem,
		Name:      "request_duration_seconds",
		Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		Help:      "Histogram of RPC client request durations",
	}, []string{
		"method",
	})
	RPCClientResponsesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: RPCClientSubsystem,
		Name:      "responses_total",
		Help:      "Total RPC request responses received by the opnode's RPC client",
	}, []string{
		"method",
		"error",
	})
)

// RecordInfo sets a pseudo-metric that contains versioning and
// config info for the opnode.
func RecordInfo(version string) {
	Info.WithLabelValues(version).Set(1)
}

// RecordUp sets the up metric to 1.
func RecordUp() {
	Up.Set(1)
}

// RecordRPCServerRequest is a helper method to record an incoming RPC
// call to the opnode's RPC server. It bumps the requests metric,
// and tracks how long it takes to serve a response.
func RecordRPCServerRequest(method string) func() {
	RPCServerRequestsTotal.WithLabelValues(method).Inc()
	timer := prometheus.NewTimer(RPCServerRequestDurationSeconds.WithLabelValues(method))
	return func() {
		timer.ObserveDuration()
	}
}

// RecordRPCClientRequest is a helper method to record an RPC client
// request. It bumps the requests metric, tracks the response
// duration, and records the response's error code.
func RecordRPCClientRequest(method string) func(err error) {
	RPCClientRequestsTotal.WithLabelValues(method).Inc()
	timer := prometheus.NewTimer(RPCClientRequestDurationSeconds.WithLabelValues(method))
	return func(err error) {
		RecordRPCClientResponse(method, err)
		timer.ObserveDuration()
	}
}

// RecordRPCClientResponse records an RPC response. It will
// convert the passed-in error into something metrics friendly.
// Nil errors get converted into <nil>, RPC errors are converted
// into rpc_<error code>, HTTP errors are converted into
// http_<status code>, and everything else is converted into
// <unknown>.
func RecordRPCClientResponse(method string, err error) {
	var errStr string
	if err == nil {
		errStr = "<nil>"
	} else if rpcErr, ok := err.(rpc.Error); ok {
		errStr = fmt.Sprintf("rpc_%d", rpcErr.ErrorCode())
	} else if httpError, ok := err.(rpc.HTTPError); ok {
		errStr = fmt.Sprintf("http_%d", httpError.StatusCode)
	} else {
		errStr = "<unknown>"
	}
	RPCClientResponsesTotal.WithLabelValues(method, errStr).Inc()
}

// Serve starts the metrics server on the given hostname and port.
// The server will be closed when the passed-in context is cancelled.
func Serve(ctx context.Context, hostname string, port int) error {
	addr := net.JoinHostPort(hostname, strconv.Itoa(port))
	server := &http.Server{
		Addr:    addr,
		Handler: promhttp.Handler(),
	}
	go func() {
		<-ctx.Done()
		server.Close()
	}()
	return server.ListenAndServe()
}
