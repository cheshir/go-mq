package mq

import (
	"fmt"
	"time"

	"github.com/NeowayLabs/wabbit"
	"github.com/NeowayLabs/wabbit/amqp"
	amqpDriver "github.com/streadway/amqp"
)

func New(config Config) (MQer, error) {
	config.normalize()

	mq := &MQ{
		errorChannel:         make(chan error, 1),
		dsn:                  config.DSN,
		internalErrorChannel: make(chan error, 1),
		producers:            newProducersRegistry(len(config.Producers)),
		reconnectTimeout:     config.ReconnectTimeout,
	}

	if err := mq.connect(); err != nil {
		return nil, err
	}

	go mq.errorHandler()

	return mq, mq.setup(config)
}

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
	producers            producersRegistry
	reconnectTimeout     time.Duration // Delay before reconnect in seconds.
}

func (mq *MQ) setup(config Config) error {
	if err := mq.setupExchanges(config.Exchanges); err != nil {
		return err
	}

	if err := mq.setupQueues(config.Queues); err != nil {
		return err
	}

	if err := mq.setupProducers(config.Producers); err != nil {
		return err
	}

	return nil
}

func (mq *MQ) setupExchanges(exchanges Exchanges) error {
	for _, config := range exchanges {
		if err := mq.declareExchange(config); err != nil {
			return err
		}
	}

	return nil
}

func (mq *MQ) declareExchange(config ExchangeConfig) error {
	return mq.channel.ExchangeDeclare(config.Name, config.Type, wabbit.Option(config.Options))
}

func (mq *MQ) setupQueues(queues Queues) error {
	for _, config := range queues {
		if err := mq.declareQueue(config); err != nil {
			return err
		}
	}

	return nil
}

func (mq *MQ) declareQueue(config QueueConfig) error {
	if _, err := mq.channel.QueueDeclare(config.Name, wabbit.Option(config.Options)); err != nil {
		return err
	}

	return mq.channel.QueueBind(config.Name, config.RoutingKey, config.Exchange, wabbit.Option(config.BindingOptions))
}

func (mq *MQ) setupProducers(producers Producers) error {
	for _, config := range producers {
		if err := mq.registerProducer(config); err != nil {
			return err
		}
	}

	return nil
}

func (mq *MQ) registerProducer(config ProducerConfig) error {

	if _, ok := mq.producers.Get(config.Name); ok {
		return fmt.Errorf(`Producer with name "%s" is already registered`, config.Name)
	}

	producer := producer{
		channel:        mq.channel,
		errorChannel:   mq.internalErrorChannel,
		exchange:       config.Exchange,
		options:        wabbit.Option(config.Options),
		publishChannel: make(chan []byte, config.BufferSize),
		routingKey:     config.RoutingKey,
	}

	go producer.worker()
	mq.producers.Set(config.Name, &producer)

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
	mq.producers.SetNewChannel(mq.channel)
}

// Error provides an ability to access occurring errors.
func (mq *MQ) Error() <-chan error {
	return mq.errorChannel
}

// GetProducer returns a producer by its name.
func (mq *MQ) GetProducer(name string) (publisher Producer, ok bool) {
	return mq.producers.Get(name)
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
