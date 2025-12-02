package mongodb

import (
	"crypto/tls"
	"time"

	"github.com/go-kratos/kratos/v2/log"
)

type options struct {
	// 基本连接信息
	URI      string `json:"uri,omitempty"`
	Database string `json:"database,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`

	// 认证相关
	AuthMechanism           string            `json:"auth_mechanism,omitempty"`            // e.g. SCRAM-SHA-1, SCRAM-SHA-256, MONGODB-X509, GSSAPI, PLAIN
	AuthMechanismProperties map[string]string `json:"auth_mechanism_properties,omitempty"` // 认证机制属性
	AuthSource              string            `json:"auth_source,omitempty"`               // e.g. admin, $external

	// 超时与连接参数（protobuf Duration -> time.Duration）
	ConnectTimeout         *time.Duration `json:"connect_timeout,omitempty"`
	HeartbeatInterval      *time.Duration `json:"heartbeat_interval,omitempty"`
	LocalThreshold         *time.Duration `json:"local_threshold,omitempty"`
	MaxConnIdleTime        *time.Duration `json:"max_conn_idle_time,omitempty"`
	MaxStaleness           *time.Duration `json:"max_staleness,omitempty"`
	ServerSelectionTimeout *time.Duration `json:"server_selection_timeout,omitempty"`
	SocketTimeout          *time.Duration `json:"socket_timeout,omitempty"`
	Timeout                *time.Duration `json:"timeout,omitempty"`

	// 可观测性
	EnableTracing bool `json:"enable_tracing,omitempty"`
	EnableMetrics bool `json:"enable_metrics,omitempty"`

	Logger *log.Helper

	TLS *tls.Config `json:"tls,omitempty"`
}

type Option func(o *options)

func WithURI(uri string) Option {
	return func(o *options) {
		o.URI = uri
	}
}

func WithDatabase(database string) Option {
	return func(o *options) {
		o.Database = database
	}
}

func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(o *options) {
		o.TLS = tlsConfig
	}
}

func WithLogger(logger log.Logger) Option {
	return func(o *options) {
		o.Logger = log.NewHelper(log.With(logger, "module", "mongodb-client"))
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(o *options) {
		o.Timeout = &timeout
	}
}

func WithConnectTimeout(connectTimeout time.Duration) Option {
	return func(o *options) {
		o.ConnectTimeout = &connectTimeout
	}
}

func WithServerSelectionTimeout(serverSelectionTimeout time.Duration) Option {
	return func(o *options) {
		o.ServerSelectionTimeout = &serverSelectionTimeout
	}
}

func WithHeartbeatInterval(interval time.Duration) Option {
	return func(o *options) {
		o.HeartbeatInterval = &interval
	}
}

func WithLocalThreshold(threshold time.Duration) Option {
	return func(o *options) {
		o.LocalThreshold = &threshold
	}
}

func WithMaxConnIdleTime(maxIdleTime time.Duration) Option {
	return func(o *options) {
		o.MaxConnIdleTime = &maxIdleTime
	}
}

func WithMaxStaleness(maxStaleness time.Duration) Option {
	return func(o *options) {
		o.MaxStaleness = &maxStaleness
	}
}

func WithSocketTimeout(socketTimeout time.Duration) Option {
	return func(o *options) {
		o.SocketTimeout = &socketTimeout
	}
}

func WithCredentials(username, password string) Option {
	return func(o *options) {
		o.Username = username
		o.Password = password
	}
}

func WithAuthMechanism(authMechanism string) Option {
	return func(o *options) {
		o.AuthMechanism = authMechanism
	}
}

func WithAuthMechanismProperties(props map[string]string) Option {
	return func(o *options) {
		o.AuthMechanismProperties = props
	}
}

func WithAuthSource(authSource string) Option {
	return func(o *options) {
		o.AuthSource = authSource
	}
}

func WithEnableTracing(enable bool) Option {
	return func(o *options) {
		o.EnableTracing = enable
	}
}

func WithEnableMetrics(enable bool) Option {
	return func(o *options) {
		o.EnableMetrics = enable
	}
}

func WithPassword(password string) Option {
	return func(o *options) {
		o.Password = password
	}
}

func WithUsername(username string) Option {
	return func(o *options) {
		o.Username = username
	}
}
