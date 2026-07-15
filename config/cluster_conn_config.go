package config

import (
	"time"

	"github.com/temporalio/temporal-proxy/pkg/validation"

	"github.com/temporalio/s2s-proxy/collect"
	"github.com/temporalio/s2s-proxy/encryption"
	"github.com/temporalio/s2s-proxy/transport/grpcutil"
)

// Looking for examples? Check ./develop/sample-cluster-conn-config.yaml
type (
	ClusterConnConfig struct {
		Name                         string              `yaml:"name"`
		Local                        ClusterDefinition   `yaml:"local"`
		Remote                       ClusterDefinition   `yaml:"remote"`
		ReplicationEndpoint          string              `yaml:"replicationEndpoint"`
		FVITranslation               IntMapping          `yaml:"failoverVersionIncrementTranslation"`
		ACLPolicy                    *ACLPolicy          `yaml:"aclPolicy"`
		NamespaceTranslation         StringTranslator    `yaml:"namespaceTranslation"`
		SearchAttributeTranslation   SATranslationConfig `yaml:"searchAttributeTranslation"`
		CustomSearchAttributeAliases CustomSAAliasConfig `yaml:"customSearchAttributeAliases"`
		RemoteClusterHealthCheck     HealthCheckConfig   `yaml:"remoteClusterHealthCheck"`
		LocalClusterHealthCheck      HealthCheckConfig   `yaml:"localClusterHealthCheck"`
		ShardCountConfig             ShardCountConfig    `yaml:"shardCount"`
		MemberlistConfig             *MemberlistConfig   `yaml:"memberlist"`
		EncryptionConfig             EncryptionConfig    `yaml:"encryption"`
	}

	StringTranslator struct {
		Mappings    []StringMapping `yaml:"mappings"`
		cachedBiMap collect.StaticBiMap[string, string]
	}

	StringMapping struct {
		Local  string `yaml:"local"`
		Remote string `yaml:"remote"`
	}

	IntMapping struct {
		Local  int64 `yaml:"local"`
		Remote int64 `yaml:"remote"`
	}

	ConnectionType string

	ClusterDefinition struct {
		ConnectionType ConnectionType   `yaml:"connectionType"`
		TcpClient      TCPTLSInfo       `yaml:"tcpClient"`
		TcpServer      TCPTLSInfo       `yaml:"tcpServer"`
		MuxCount       int              `yaml:"muxCount"`
		MuxAddressInfo TCPTLSInfo       `yaml:"muxAddressInfo"`
		GRPCClient     GRPCClientConfig `yaml:"grpcClient"`
	}

	// GRPCClientConfig tunes the gRPC client this proxy uses to reach the cluster's local
	// Temporal frontend (and, for the mux transports, the multiplexed client). All fields are
	// optional; zero values fall back to the grpcutil defaults. Tuning these matters when the
	// proxy runs behind a service mesh (Istio/Envoy) whose sidecar cycles or resets connections.
	// Durations are expressed in milliseconds to stay consistent with the rest of this config.
	GRPCClientConfig struct {
		// ConnectTimeoutMs bounds a single connection attempt. Lower it (e.g. 5000) behind a
		// mesh sidecar so a reset connection reconnects quickly instead of stalling ~20s.
		ConnectTimeoutMs int `yaml:"connectTimeoutMs"`
		// KeepAliveTimeMs is the interval between client keepalive pings (0 = default).
		KeepAliveTimeMs int `yaml:"keepAliveTimeMs"`
		// KeepAliveTimeoutMs is the ping-ack timeout before the connection is considered dead.
		KeepAliveTimeoutMs int `yaml:"keepAliveTimeoutMs"`
		// KeepAlivePermitWithoutStream keeps idle connections warm by pinging with no active
		// RPCs. Enable only if the server's keepalive enforcement policy allows it.
		KeepAlivePermitWithoutStream bool `yaml:"keepAlivePermitWithoutStream"`
	}

	TCPTLSInfo struct {
		ConnectionString string               `yaml:"address"`
		TLSConfig        encryption.TLSConfig `yaml:"tls"`
	}

	ShardCountConfig struct {
		Mode             ShardCountMode `yaml:"mode"`
		LocalShardCount  int32          `yaml:"localShardCount"`
		RemoteShardCount int32          `yaml:"remoteShardCount"`
	}
)

const (
	ConnTypeTCP       ConnectionType = "tcp"
	ConnTypeMuxServer ConnectionType = "mux-server"
	ConnTypeMuxClient ConnectionType = "mux-client"
)

// ToClientOptions converts the YAML-facing config into grpcutil.ClientOptions, translating the
// millisecond fields into durations. Zero values are preserved so grpcutil applies its defaults.
func (c GRPCClientConfig) ToClientOptions() grpcutil.ClientOptions {
	return grpcutil.ClientOptions{
		ConnectTimeout:               time.Duration(c.ConnectTimeoutMs) * time.Millisecond,
		KeepAliveTime:                time.Duration(c.KeepAliveTimeMs) * time.Millisecond,
		KeepAliveTimeout:             time.Duration(c.KeepAliveTimeoutMs) * time.Millisecond,
		KeepAlivePermitWithoutStream: c.KeepAlivePermitWithoutStream,
	}
}

func (config *StringTranslator) AsLocalToRemoteBiMap() (collect.StaticBiMap[string, string], error) {
	if config.cachedBiMap != nil {
		return config.cachedBiMap, nil
	}
	mapping, err := collect.NewStaticBiMap(func(yield func(string, string) bool) {
		for _, mapping := range config.Mappings {
			if !yield(mapping.Local, mapping.Remote) {
				return
			}
		}
	}, len(config.Mappings))
	if err != nil {
		return nil, err
	}
	config.cachedBiMap = mapping
	return config.cachedBiMap, nil
}

// Validate reports problems in this connection's config. Only the encryption
// block is covered so far; the checks NewProxy makes inline could move here.
func (c *ClusterConnConfig) Validate() error {
	return validation.Validate(
		"",
		validation.Nested("encryption", &c.EncryptionConfig),
	)
}
