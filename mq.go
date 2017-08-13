// Package mq provides an ability to integrate with message broker via AMQP in a declarative way.
package mq

import (
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/NeowayLabs/wabbit"
	"github.com/NeowayLabs/wabbit/amqp"
	"github.com/NeowayLabs/wabbit/amqptest"
	"github.com/NeowayLabs/wabbit/utils"
	amqpDriver "github.com/streadway/amqp"
)

const (
	// Describes states during reconnect.
	statusReadyForReconnect int32 = 0
	statusReconnecting            = 1
)

// Used for creating connection to the fake AMQP server for tests.
var brokerIsMocked bool

// MQ describes methods provided by message broker adapter.
type MQ interface {
	GetConsumer(name string) (Consumer, error)
	SetConsumerHandler(name string, handler ConsumerHandler) error
	GetProducer(name string) (Producer, error)
	Error() <-chan error
	Close()
}

type conn interface {
	Channel() (wabbit.Channel, error)
	Close() error
	NotifyClose(chan wabbit.Error) chan wabbit.Error
}

type mq struct {
	channel              wabbit.Channel
	config               Config
	connection           conn
	errorChannel         chan error
	internalErrorChannel chan error
	consumers            *consumersRegistry
	producers            *producersRegistry
	reconnectStatus      int32 // Defines whether client is trying to reconnect or not.
}

// New initializes AMQP connection to the message broker
// and returns adapter that provides an ability
// to get configured consumers and producers, read occurred errors and shutdown all workers.
func New(config Config) (MQ, error) {
	config.normalize()

	mq := &mq{
		config:               config,
		errorChannel:         make(chan error),
		internalErrorChannel: make(chan error),
		consumers:            newConsumersRegistry(len(config.Consumers)),
		producers:            newProducersRegistry(len(config.Producers)),
	}

	if err := mq.connect(); err != nil {
		return nil, err
	}

	go mq.errorHandler()

	return mq, mq.initialSetup()
}

// GetConsumer returns a consumer by its name or error if consumer wasn't found.
func (mq *mq) GetConsumer(name string) (consumer Consumer, err error) {
	consumer, ok := mq.consumers.Get(name)
	if !ok {
		err = fmt.Errorf("Consumer '%s' is not registered. Check your configuration.", name)
	}

	return
}

// Set handler for consumer by its name. Returns false if consumer wasn't found.
func (mq *mq) SetConsumerHandler(name string, handler ConsumerHandler) error {
	consumer, err := mq.GetConsumer(name)
	if err != nil {
		return err
	}

	consumer.Consume(handler)

	return nil
}

// GetProducer returns a producer by its name or false if producer wasn't found.
func (mq *mq) GetProducer(name string) (producer Producer, err error) {
	producer, ok := mq.producers.Get(name)
	if !ok {
		err = fmt.Errorf("Producer '%s' is not registered. Check your configuration,", name)
	}

	return
}

// Error provides an ability to access occurring errors.
func (mq *mq) Error() <-chan error {
	return mq.errorChannel
}

// Shutdown all workers and close connection to the message broker.
func (mq *mq) Close() {
	mq.stopProducersAndConsumers()

	if mq.channel != nil {
		mq.channel.Close()
	}

	if mq.connection != nil {
		mq.connection.Close()
	}
}

func (mq *mq) connect() error {
	connection, err := mq.createConnection()
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

	go mq.handleCloseEvent()

	return nil
}

func (mq *mq) createConnection() (conn conn, err error) {
	if brokerIsMocked {
		return amqptest.Dial(mq.config.DSN)
	}

	return amqp.Dial(mq.config.DSN)
}

// Register close handler.
// To get more details visit https://godoc.org/github.com/streadway/amqp#Connection.NotifyClose.
func (mq *mq) handleCloseEvent() {
	err := <-mq.connection.NotifyClose(make(chan wabbit.Error))
	if err != nil {
		mq.internalErrorChannel <- err
	}
}

func (mq *mq) errorHandler() {
	for err := range mq.internalErrorChannel {
		mq.errorChannel <- err // Proxies errors to the user.
		mq.processError(err)
	}
}

func (mq *mq) processError(err interface{}) {
	switch err.(type) {
	case *net.OpError:
		go mq.reconnect()
	case *utils.Error: // Broken connection. Used in tests.
		go mq.reconnect()
	case *amqpDriver.Error:
		rmqErr, _ := err.(*amqpDriver.Error)
		if rmqErr.Server == false { // For example channel was closed.
			go mq.reconnect()
		}
	default:
		// There is no special behaviour for other errors.
	}
}

func (mq *mq) initialSetup() error {
	if err := mq.setupExchanges(); err != nil {
		return err
	}

	if err := mq.setupQueues(); err != nil {
		return err
	}

	if err := mq.setupProducers(); err != nil {
		return err
	}

	return mq.setupConsumers()
}

// Called after each reconnect to recreate non-durable queues and exchanges.
func (mq *mq) setupAfterReconnect() error {
	if err := mq.setupExchanges(); err != nil {
		return err
	}

	if err := mq.setupQueues(); err != nil {
		return err
	}

	mq.producers.GoEach(func(producer *producer) {
		if err := mq.reconnectProducer(producer); err != nil {
			mq.internalErrorChannel <- err
		}
	})

	mq.consumers.GoEach(func(consumer *consumer) {
		if err := mq.reconnectConsumer(consumer); err != nil {
			mq.internalErrorChannel <- err
		}
	})

	return nil
}

func (mq *mq) setupExchanges() error {
	for _, config := range mq.config.Exchanges {
		if err := mq.declareExchange(config); err != nil {
			return err
		}
	}

	return nil
}

func (mq *mq) declareExchange(config ExchangeConfig) error {
	return mq.channel.ExchangeDeclare(config.Name, config.Type, wabbit.Option(config.Options))
}

func (mq *mq) setupQueues() error {
	for _, config := range mq.config.Queues {
		if err := mq.declareQueue(config); err != nil {
			return err
		}
	}

	return nil
}

func (mq *mq) declareQueue(config QueueConfig) error {
	if _, err := mq.channel.QueueDeclare(config.Name, wabbit.Option(config.Options)); err != nil {
		return err
	}

	return mq.channel.QueueBind(config.Name, config.RoutingKey, config.Exchange, wabbit.Option(config.BindingOptions))
}

func (mq *mq) setupProducers() error {
	for _, config := range mq.config.Producers {
		if err := mq.registerProducer(config); err != nil {
			return err
		}
	}

	return nil
}

func (mq *mq) registerProducer(config ProducerConfig) error {
	if _, ok := mq.producers.Get(config.Name); ok {
		return fmt.Errorf(`Producer with name "%s" is already registered`, config.Name)
	}

	channel, err := mq.connection.Channel()
	if err != nil {
		return err
	}

	producer := newProducer(channel, mq.internalErrorChannel, config)

	go producer.worker()
	mq.producers.Set(config.Name, producer)

	return nil
}

func (mq *mq) reconnectProducer(producer *producer) error {
	channel, err := mq.connection.Channel()
	if err != nil {
		return err
	}

	producer.setChannel(channel)
	go producer.worker()

	return nil
}

func (mq *mq) setupConsumers() error {
	for _, config := range mq.config.Consumers {
		if err := mq.registerConsumer(config); err != nil {
			return err
		}
	}

	return nil
}

func (mq *mq) registerConsumer(config ConsumerConfig) error {
	if _, ok := mq.consumers.Get(config.Name); ok {
		return fmt.Errorf(`Consumer with name "%s" is already registered`, config.Name)
	}

	// Consumer must have at least one worker.
	if config.Workers == 0 {
		config.Workers = 1
	}

	consumer := newConsumer(config) // We need to save a whole config for reconnect.
	consumer.prefetchCount = config.PrefetchCount
	consumer.prefetchSize = config.PrefetchSize

	for i := 0; i < config.Workers; i++ {
		worker := newWorker(mq.internalErrorChannel)

		if err := mq.initializeConsumersWorker(consumer, worker); err != nil {
			return err
		}

		consumer.workers[i] = worker
	}

	mq.consumers.Set(config.Name, consumer) // Workers will start after consumer.Consume method call.

	return nil
}

func (mq *mq) reconnectConsumer(consumer *consumer) error {
	for _, worker := range consumer.workers {
		if err := mq.initializeConsumersWorker(consumer, worker); err != nil {
			return err
		}

		go worker.Run(consumer.handler)
	}

	return nil
}

func (mq *mq) initializeConsumersWorker(consumer *consumer, worker *worker) error {
	channel, err := mq.connection.Channel()
	if err != nil {
		return err
	}

	if err := channel.Qos(consumer.prefetchCount, consumer.prefetchSize, false); err != nil {
		return err
	}

	deliveries, err := channel.Consume(consumer.queue, "", consumer.options)
	if err != nil {
		return err
	}

	worker.setChannel(channel)
	worker.deliveries = deliveries

	return nil
}

// Reconnect stops current producers and consumers,
// recreates connection to the rabbit and than runs producers and consumers.
func (mq *mq) reconnect() {
	notBusy := atomic.CompareAndSwapInt32(&mq.reconnectStatus, statusReadyForReconnect, statusReconnecting)
	if !notBusy {
		// There is no need to start a new reconnect if the previous one is not finished yet.
		return
	}

	defer func() {
		atomic.StoreInt32(&mq.reconnectStatus, statusReadyForReconnect)
	}()

	time.Sleep(mq.config.ReconnectDelay) // TODO Add incremental sleep.

	mq.stopProducersAndConsumers()

	if err := mq.connect(); err != nil {
		mq.internalErrorChannel <- err

		return
	}

	if err := mq.setupAfterReconnect(); err != nil {
		mq.internalErrorChannel <- err
	}
}

func (mq *mq) stopProducersAndConsumers() {
	mq.producers.GoEach(func(producer *producer) {
		producer.Stop()
	})

	mq.consumers.GoEach(func(consumer *consumer) {
		consumer.Stop()
	})
}
