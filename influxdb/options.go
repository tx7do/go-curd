package influxdb

import (
	"crypto/tls"
	"time"

	"github.com/go-kratos/kratos/v2/log"
)

type options struct {
	Host                  string        `json:"host,omitempty"`                    // 主机地址
	Token                 string        `json:"token,omitempty"`                   // 认证令牌
	AuthScheme            string        `json:"auth_scheme,omitempty"`             // 认证方案：default、basic
	Proxy                 string        `json:"proxy,omitempty"`                   // 代理地址
	Organization          string        `json:"organization,omitempty"`            // 组织名
	Database              string        `json:"database,omitempty"`                // 数据库名
	Timeout               time.Duration `json:"timeout,omitempty"`                 // 连接超时时间
	IdleConnectionTimeout time.Duration `json:"idle_connection_timeout,omitempty"` // 空闲连接超时时间
	MaxIdleConnections    int           `json:"max_idle_connections,omitempty"`    // 连接池最大空闲连接数

	Logger *log.Helper

	TLS *tls.Config `json:"tls,omitempty"`
}

type Option func(o *options)

func WithHost(host string) Option {
	return func(o *options) {
		o.Host = host
	}
}

func WithToken(token string) Option {
	return func(o *options) {
		o.Token = token
	}
}

func WithOrganization(organization string) Option {
	return func(o *options) {
		o.Organization = organization
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
		o.Logger = log.NewHelper(log.With(logger, "module", "influxdb-client"))
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(o *options) {
		o.Timeout = timeout
	}
}

func WithIdleConnectionTimeout(idleTimeout time.Duration) Option {
	return func(o *options) {
		o.IdleConnectionTimeout = idleTimeout
	}
}
func WithMaxIdleConnections(maxIdle int) Option {
	return func(o *options) {
		o.MaxIdleConnections = maxIdle
	}
}
