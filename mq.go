package mq

import (
	"fmt"
	"time"

	"github.com/NeowayLabs/wabbit"
	"github.com/NeowayLabs/wabbit/amqp"
	"github.com/spf13/viper"
	amqpDriver "github.com/streadway/amqp"
)

type MQer interface {
	GetProducer(name string) (Producer, bool)
	Error() <-chan error
	Close()
}

type MQ struct {
	channel              wabbit.Channel
	connection           wabbit.Conn
	errorChannel         chan error
	dsn                  string // We need to store it for reconnect.
	internalErrorChannel chan error
	producers            map[string]*producer
	reconnectTimeout     time.Duration // Delay before reconnect in seconds.
}

func New(config *viper.Viper) (MQer, error) {
	mq := &MQ{
		errorChannel:         make(chan error, 1),
		dsn:                  config.GetString("dsn"),
		internalErrorChannel: make(chan error, 1),
		producers:            make(map[string]*producer),
		reconnectTimeout:     time.Duration(config.GetInt("reconnect_timeout")) * time.Second,
	}

	if err := mq.connect(); err != nil {
		return nil, err
	}

	go mq.errorHandler()

	return mq, mq.readConfig(config)
}

type configReaders struct {
	ConfigKey string                                       // Root key for a block of MQ configuration.
	Read      func(name string, config *viper.Viper) error // Function that process block of configuration.
}

func (mq *MQ) readConfig(config *viper.Viper) error {
	// We declare this list inside the function because we need access to private methods.
	readers := [3]configReaders{
		{ConfigKey: "exchanges", Read: mq.declareExchange},
		{ConfigKey: "queues", Read: mq.declareQueue},
		{ConfigKey: "producers", Read: mq.registerProducer},
	}

	for _, reader := range readers {
		readerConfig := config.Sub(reader.ConfigKey)

		// Ignore nonexistent blocks of configuration.
		if readerConfig == nil {
			continue
		}

		// Each configuration block represents collection of configurations
		// for single type components like queues or consumers
		// where key is a name of component and value is its configuration.
		for name := range readerConfig.AllSettings() {
			if err := reader.Read(name, readerConfig.Sub(name)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (mq *MQ) declareExchange(name string, config *viper.Viper) error {
	exchangeType := config.GetString("type")
	exchangeOptions := fixCapitalization(config.GetStringMap("options"))

	return mq.channel.ExchangeDeclare(name, exchangeType, exchangeOptions)
}

func (mq *MQ) declareQueue(name string, config *viper.Viper) error {
	exchange := config.GetString("exchange")
	routingKey := config.GetString("routing_key")
	options := fixCapitalization(config.GetStringMap("options"))
	bindingOptions := fixCapitalization(config.GetStringMap("binding_options"))

	if _, err := mq.channel.QueueDeclare(name, options); err != nil {
		return err
	}

	return mq.channel.QueueBind(name, routingKey, exchange, bindingOptions)
}

func (mq *MQ) registerProducer(name string, config *viper.Viper) error {
	if _, ok := mq.producers[name]; ok {
		return fmt.Errorf(`Producer with name "%s" already exists`, name)
	}

	producer := &producer{
		channel:        mq.channel,
		errorChannel:   mq.internalErrorChannel,
		exchange:       config.GetString("exchange"),
		options:        fixCapitalization(config.GetStringMap("publish_options")),
		publishChannel: make(chan []byte, config.GetInt("buffer_size")),
		routingKey:     config.GetString("routing_key"),
	}

	go producer.worker()
	mq.producers[name] = producer

	return nil
}

func (mq *MQ) connect() error {
	connection, err := amqp.Dial(mq.dsn)
	if err != nil {
		return err
	}

	channel, err := connection.Channel()
	if err != nil {
		connection.Close()

		return err
	}

	mq.connection = connection
	mq.channel = channel

	return nil
}

func (mq *MQ) errorHandler() {
	for err := range mq.internalErrorChannel {
		select {
		case mq.errorChannel <- err:
			mq.processError(err)
		default: // Drop errors if channel buffer is full.
		}
	}
}

func (mq *MQ) processError(err interface{}) {
	if rmqErr, ok := err.(*amqpDriver.Error); ok {
		if rmqErr.Server == false { // For example channel was closed.
			time.Sleep(mq.reconnectTimeout)
			mq.reconnect()
		}
	}
}

func (mq *MQ) reconnect() {
	mq.connect()

	for _, producer := range mq.producers {
		producer.setChannel(mq.channel)
	}
}

// You can check for errors by reading errors from this channel.
func (mq *MQ) Error() <-chan error {
	return mq.errorChannel
}

func (mq *MQ) GetProducer(name string) (publisher Producer, ok bool) {
	publisher, ok = mq.producers[name]

	return
}

// TODO stop producers and consumers.
func (mq *MQ) Close() {
	if mq.channel != nil {
		mq.channel.Close()
	}

	if mq.connection != nil {
		mq.connection.Close()
	}
}
