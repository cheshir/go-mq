package mq

import "time"

type DeliveryMode int

// List of available values for `delivery_mode` producer option.
const (
	NonPersistent DeliveryMode = 1
	Persistent                 = 2
)

type Config struct {
	DSN              string        `mapstructure:"dsn" json:"dsn" yaml:"dsn"`
	ReconnectTimeout time.Duration `mapstructure:"reconnect_timeout" json:"reconnect_timeout" yaml:"reconnect_timeout"`
	Exchanges        Exchanges     `mapstructure:"exchanges" json:"exchanges" yaml:"exchanges"`
	Queues           Queues        `mapstructure:"queues" json:"queues" yaml:"queues"`
	Producers        Producers     `mapstructure:"producers" json:"producers" yaml:"producers"`
}

// Traverses the config tree and fixes option keys name.
func (config Config) normalize() {
	config.Exchanges.normalize()
	config.Queues.normalize()
	config.Producers.normalize()
}

type Exchanges []ExchangeConfig

func (exchanges Exchanges) normalize() {
	for _, exchange := range exchanges {
		exchange.normalize()
	}
}

type ExchangeConfig struct {
	Name    string  `mapstructure:"name" json:"name" yaml:"name"`
	Type    string  `mapstructure:"type" json:"type" yaml:"type"`
	Options Options `mapstructure:"options" json:"options" yaml:"options"`
}

func (config ExchangeConfig) normalize() {
	config.Options.normalizeKeys()
}

type Queues []QueueConfig

func (queues Queues) normalize() {
	for _, queue := range queues {
		queue.normalize()
	}
}

type QueueConfig struct {
	Exchange       string  `mapstructure:"exchange" json:"exchange" yaml:"exchange"`
	Name           string  `mapstructure:"name" json:"name" yaml:"name"`
	RoutingKey     string  `mapstructure:"routing_key" json:"routing_key" yaml:"routing_key"`
	BindingOptions Options `mapstructure:"binding_options" json:"binding_options" yaml:"binding_options"`
	Options        Options `mapstructure:"options" json:"options" yaml:"options"`
}

func (config QueueConfig) normalize() {
	config.BindingOptions.normalizeKeys()
	config.Options.normalizeKeys()
}

type Producers []ProducerConfig

func (producers Producers) normalize() {
	for _, producer := range producers {
		producer.normalize()
	}
}

type ProducerConfig struct {
	BufferSize int     `mapstructure:"buffer_size" json:"buffer_size" yaml:"buffer_size"`
	Exchange   string  `mapstructure:"exchange" json:"exchange" yaml:"exchange"`
	Name       string  `mapstructure:"name" json:"name" yaml:"name"`
	RoutingKey string  `mapstructure:"routing_key" json:"routing_key" yaml:"routing_key"`
	Options    Options `mapstructure:"options" json:"options" yaml:"options"`
}

func (config ProducerConfig) normalize() {
	config.Options.normalizeKeys()
}

type Options map[string]interface{}

// Map from lowercase option name to the expected name.
var capitalizationMap = map[string]string{
	"autodelete":       "autoDelete",
	"auto_delete":      "autoDelete",
	"contentencoding":  "contentEncoding",
	"content_encoding": "contentEncoding",
	"contenttype":      "contentType",
	"content_type":     "contentType",
	"deliverymode":     "deliveryMode",
	"delivery_mode":    "deliveryMode",
	"noack":            "noAck",
	"no_ack":           "noAck",
	"nolocal":          "noLocal",
	"no_local":         "noLocal",
	"nowait":           "noWait",
	"no_wait":          "noWait",
}

// By default yaml reader unmarshals keys in lowercase,
// but AMQP client looks for keys in camelcase,
// so we're going to fix this issue.
func (options Options) normalizeKeys() {
	for name, value := range options {
		if correctName, needFix := capitalizationMap[name]; needFix {
			delete(options, name)
			options[correctName] = value
		}
	}
}
