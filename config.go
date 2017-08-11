package mq

import "time"

// DeliveryMode describes an AMQP message delivery mode.
type DeliveryMode int

// List of available values for `delivery_mode` producer option.
const (
	NonPersistent DeliveryMode = 1
	Persistent                 = 2
)

// Config describes all available options for amqp connection creation.
type Config struct {
	DSN            string        `mapstructure:"dsn" json:"dsn" yaml:"dsn"`
	ReconnectDelay time.Duration `mapstructure:"reconnect_delay" json:"reconnect_delay" yaml:"reconnect_delay"`
	Exchanges      Exchanges     `mapstructure:"exchanges" json:"exchanges" yaml:"exchanges"`
	Queues         Queues        `mapstructure:"queues" json:"queues" yaml:"queues"`
	Producers      Producers     `mapstructure:"producers" json:"producers" yaml:"producers"`
	Consumers      Consumers     `mapstructure:"consumers" json:"consumers" yaml:"consumers"`
}

// Traverses the config tree and fixes option keys name.
func (config Config) normalize() {
	config.Exchanges.normalize()
	config.Queues.normalize()
	config.Producers.normalize()
	config.Consumers.normalize()
}

// Exchanges describes configuration list for exchanges.
type Exchanges []ExchangeConfig

func (exchanges Exchanges) normalize() {
	for _, exchange := range exchanges {
		exchange.normalize()
	}
}

// ExchangeConfig describes exchange's configuration.
type ExchangeConfig struct {
	Name    string  `mapstructure:"name" json:"name" yaml:"name"`
	Type    string  `mapstructure:"type" json:"type" yaml:"type"`
	Options Options `mapstructure:"options" json:"options" yaml:"options"`
}

func (config ExchangeConfig) normalize() {
	config.Options.normalizeKeys()
}

// Queues describes configuration list for queues.
type Queues []QueueConfig

func (queues Queues) normalize() {
	for _, queue := range queues {
		queue.normalize()
	}
}

// QueueConfig describes queue's configuration.
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

// Producers describes configuration list for producers.
type Producers []ProducerConfig

func (producers Producers) normalize() {
	for _, producer := range producers {
		producer.normalize()
	}
}

// ProducerConfig describes producer's configuration.
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

// Consumers describes configuration list for consumers.
type Consumers []ConsumerConfig

func (consumers Consumers) normalize() {
	for _, consumer := range consumers {
		consumer.normalize()
	}
}

// ConsumerConfig describes consumer's configuration.
type ConsumerConfig struct {
	Name          string  `mapstructure:"name" json:"name" yaml:"name"`
	Queue         string  `mapstructure:"queue" json:"queue" yaml:"queue"`
	Workers       int     `mapstructure:"workers" json:"workers" yaml:"workers"`
	Options       Options `mapstructure:"options" json:"options" yaml:"options"`
	PrefetchCount int     `mapstructure:"prefetch_count" json:"prefetch_count" yaml:"prefetch_count"`
	PrefetchSize  int     `mapstructure:"prefetch_size" json:"prefetch_size" yaml:"prefetch_size"`
}

func (config ConsumerConfig) normalize() {
	config.Options.normalizeKeys()
}

// Options describes optional configuration.
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
