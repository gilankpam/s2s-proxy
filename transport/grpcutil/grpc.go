package grpcutil

import (
	"crypto/tls"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/keepalive"

	"github.com/temporalio/s2s-proxy/proto/compat"
)

const (
	// DefaultServiceConfig is a default gRPC connection service config which enables DNS round robin between IPs.
	// To use DNS resolver, a "dns:///" prefix should be applied to the hostPort.
	// https://github.com/grpc/grpc/blob/master/doc/naming.md
	DefaultServiceConfig = `{"loadBalancingConfig": [{"round_robin":{}}]}`

	// MaxBackoffDelay is a maximum interval between reconnect attempts.
	MaxBackoffDelay = 10 * time.Second

	// DefaultConnectTimeout is the minimum amount of time we are willing to give a single
	// connection attempt to complete before the next attempt starts. Behind a service mesh
	// (e.g. Istio/Envoy) the sidecar can reset an idle connection, and a large value here turns
	// every reconnect into a multi-second stall; lower it via ClientOptions.ConnectTimeout in
	// those environments.
	DefaultConnectTimeout = 20 * time.Second

	// DefaultKeepAliveTime is the default interval between client keepalive pings.
	DefaultKeepAliveTime = 30 * time.Second

	// DefaultKeepAliveTimeout is the default time to wait for a keepalive ping ack before the
	// connection is considered dead and closed.
	DefaultKeepAliveTimeout = 10 * time.Second

	// maxInternodeRecvPayloadSize indicates the internode max receive payload size.
	maxInternodeRecvPayloadSize = 128 * 1024 * 1024 // 128 Mb
)

// ClientOptions carries tunables for the gRPC client connections created by MakeDialOptions.
// The zero value is valid: any unset (non-positive) field falls back to the Default* values
// above. These matter mainly when the proxy runs behind a service mesh whose sidecar cycles or
// resets connections.
type ClientOptions struct {
	// ConnectTimeout bounds how long a single connection attempt may take before the gRPC
	// backoff loop starts the next one. Lower it (e.g. 5s) behind a mesh sidecar so a reset
	// connection reconnects quickly instead of stalling for the full default.
	ConnectTimeout time.Duration
	// KeepAliveTime is the interval between client keepalive pings.
	KeepAliveTime time.Duration
	// KeepAliveTimeout is how long to wait for a keepalive ping ack before closing the connection.
	KeepAliveTimeout time.Duration
	// KeepAlivePermitWithoutStream sends keepalive pings even when there are no active RPCs,
	// which keeps otherwise-idle connections warm (useful behind a mesh sidecar that reaps idle
	// connections). Only enable it if the server's keepalive enforcement policy permits it,
	// otherwise the server may respond with GOAWAY(ENHANCE_YOUR_CALM).
	KeepAlivePermitWithoutStream bool
}

func (o ClientOptions) withDefaults() ClientOptions {
	if o.ConnectTimeout <= 0 {
		o.ConnectTimeout = DefaultConnectTimeout
	}
	if o.KeepAliveTime <= 0 {
		o.KeepAliveTime = DefaultKeepAliveTime
	}
	if o.KeepAliveTimeout <= 0 {
		o.KeepAliveTimeout = DefaultKeepAliveTimeout
	}
	return o
}

func MakeDialOptions(tlsConfig *tls.Config, clientMetrics *grpcprom.ClientMetrics, opts ClientOptions) []grpc.DialOption {
	opts = opts.withDefaults()

	var grpcSecureOpt grpc.DialOption
	if tlsConfig == nil {
		grpcSecureOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
	} else {
		grpcSecureOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	// gRPC maintains a connection pool inside grpc.ClientConn with an auto-reconnect feature
	// that uses exponential backoff:
	// https://github.com/grpc/grpc/blob/master/doc/connection-backoff.md.
	// Default MaxDelay is 120 seconds which is too high. MinConnectTimeout bounds how long a
	// single attempt may stall before the next one begins.
	cp := grpc.ConnectParams{
		Backoff:           backoff.DefaultConfig,
		MinConnectTimeout: opts.ConnectTimeout,
	}
	cp.Backoff.MaxDelay = MaxBackoffDelay

	dialOptions := []grpc.DialOption{
		grpcSecureOpt,
		grpc.WithDefaultCallOptions(
			grpc.ForceCodecV2(encoding.GetCodecV2(compat.CodecName)),
			grpc.MaxCallRecvMsgSize(maxInternodeRecvPayloadSize),
		),
		grpc.WithDefaultServiceConfig(DefaultServiceConfig),
		grpc.WithDisableServiceConfig(),
		grpc.WithConnectParams(cp),
		// Keepalive keeps connections healthy and lets the client detect a dropped connection
		// promptly instead of only discovering it on the next RPC (which, combined with the
		// connect timeout, is what produces multi-second stalls behind a mesh sidecar).
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                opts.KeepAliveTime,
			Timeout:             opts.KeepAliveTimeout,
			PermitWithoutStream: opts.KeepAlivePermitWithoutStream,
		}),
		grpc.WithUnaryInterceptor(clientMetrics.UnaryClientInterceptor()),
		grpc.WithStreamInterceptor(clientMetrics.StreamClientInterceptor()),
	}
	return dialOptions
}
