package elasticsearch

import (
	"crypto/tls"
	"time"

	"github.com/go-kratos/kratos/v2/log"
)

type options struct {
	Addresses              []string `json:"addresses,omitempty"`
	Username               string   `json:"username,omitempty"`
	Password               string   `json:"password,omitempty"`
	CloudID                string   `json:"cloud_id,omitempty"`
	APIKey                 string   `json:"api_key,omitempty"`
	ServiceToken           string   `json:"service_token,omitempty"`
	CertificateFingerprint string   `json:"certificate_fingerprint,omitempty"`

	DisableRetry bool `json:"disable_retry,omitempty"`
	MaxRetries   int  `json:"max_retries,omitempty"`

	CompressRequestBody      bool `json:"compress_request_body,omitempty"`
	CompressRequestBodyLevel int  `json:"compress_request_body_level,omitempty"`
	PoolCompressor           bool `json:"pool_compressor,omitempty"`

	DiscoverNodesOnStart  bool          `json:"discover_nodes_on_start,omitempty"`
	DiscoverNodesInterval time.Duration `json:"discover_nodes_interval,omitempty"`

	EnableMetrics           bool `json:"enable_metrics,omitempty"`
	EnableDebugLogger       bool `json:"enable_debug_logger,omitempty"`
	EnableCompatibilityMode bool `json:"enable_compatibility_mode,omitempty"`
	DisableMetaHeader       bool `json:"disable_meta_header,omitempty"`

	TLS *tls.Config `json:"tls,omitempty"`

	Logger *log.Helper
}

type Option func(o *options)

func WithAddresses(addresses ...string) Option {
	return func(o *options) {
		o.Addresses = addresses
	}
}

func WithUsername(username string) Option {
	return func(o *options) {
		o.Username = username
	}
}

func WithPassword(password string) Option {
	return func(o *options) {
		o.Password = password
	}
}

func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(o *options) {
		o.TLS = tlsConfig
	}
}

func WithEnableMetrics(enable bool) Option {
	return func(o *options) {
		o.EnableMetrics = enable
	}
}

func WithEnableDebugLogger(enable bool) Option {
	return func(o *options) {
		o.EnableDebugLogger = enable
	}
}
func WithEnableCompatibilityMode(enable bool) Option {
	return func(o *options) {
		o.EnableCompatibilityMode = enable
	}
}

func WithDisableMetaHeader(disable bool) Option {
	return func(o *options) {
		o.DisableMetaHeader = disable
	}
}

func WithDiscoverNodesOnStart(enable bool) Option {
	return func(o *options) {
		o.DiscoverNodesOnStart = enable
	}
}
func WithDiscoverNodesInterval(interval time.Duration) Option {
	return func(o *options) {
		o.DiscoverNodesInterval = interval
	}
}

func WithDisableRetry(disable bool) Option {
	return func(o *options) {
		o.DisableRetry = disable
	}
}
func WithMaxRetries(maxRetries int) Option {
	return func(o *options) {
		o.MaxRetries = maxRetries
	}
}
func WithCompressRequestBody(enable bool) Option {
	return func(o *options) {
		o.CompressRequestBody = enable
	}
}
func WithCompressRequestBodyLevel(level int) Option {
	return func(o *options) {
		o.CompressRequestBodyLevel = level
	}
}
func WithPoolCompressor(enable bool) Option {
	return func(o *options) {
		o.PoolCompressor = enable
	}
}
func WithCloudID(cloudID string) Option {
	return func(o *options) {
		o.CloudID = cloudID
	}
}
func WithAPIKey(apiKey string) Option {
	return func(o *options) {
		o.APIKey = apiKey
	}
}
func WithServiceToken(serviceToken string) Option {
	return func(o *options) {
		o.ServiceToken = serviceToken
	}
}
func WithCertificateFingerprint(fingerprint string) Option {
	return func(o *options) {
		o.CertificateFingerprint = fingerprint
	}
}

func WithLogger(logger log.Logger) Option {
	return func(o *options) {
		o.Logger = log.NewHelper(log.With(logger, "module", "elasticsearch-client"))
	}
}
