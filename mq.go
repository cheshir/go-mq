// Package mq provides an ability to integrate with message broker via AMQP in a declarative way.
package mq

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NeowayLabs/wabbit"
	"github.com/NeowayLabs/wabbit/amqp"
	"github.com/NeowayLabs/wabbit/amqptest"
	"github.com/NeowayLabs/wabbit/utils"
	amqpDriver "github.com/rabbitmq/amqp091-go"
)

const (
	// Describes states during reconnect.
	statusReadyForReconnect int32 = 0
	statusReconnecting      int32 = 1

	ConnectionStateDisconnected ConnectionState = 1
	ConnectionStateConnected    ConnectionState = 2
	ConnectionStateConnecting   ConnectionState = 3
)

// Used for creating connection to the fake AMQP server for tests.
var brokerIsMocked bool

type conn interface {
	Channel() (wabbit.Channel, error)
	Close() error
	NotifyClose(chan wabbit.Error) chan wabbit.Error
}

type ConnectionState uint8

type MessageQueue struct {
	channel              wabbit.Channel
	config               Config
	connection           conn
	errorChannel         chan error
	internalErrorChannel chan error
	consumers            *consumersRegistry
	producers            *producersRegistry
	reconnectStatus      int32 // Defines whether client is trying to reconnect or not.
	cluster              struct {
		sync.Once
		currentNode int32
	}
	state *int32
}

// New initializes AMQP connection to the message broker
// and returns adapter that provides an ability
// to get configured consumers and producers, read occurred errors and shutdown all workers.
func New(config Config) (*MessageQueue, error) {
	config.normalize()

	mq := &MessageQueue{
		config:               config,
		errorChannel:         make(chan error),
		internalErrorChannel: make(chan error),
		consumers:            newConsumersRegistry(len(config.Consumers)),
		producers:            newProducersRegistry(len(config.Producers)),
		state:                new(int32),
	}
	atomic.StoreInt32(mq.state, int32(ConnectionStateDisconnected))

	if err := mq.connect(); err != nil {
		return nil, err
	}

	go mq.errorHandler()

	return mq, mq.initialSetup()
}

// SetConsumerHandler allows you to set handler callback without getting consumer.
// Returns false if consumer wasn't found.
// Can be called once for each consumer.
func (mq *MessageQueue) SetConsumerHandler(name string, handler ConsumerHandler) error {
	consumer, err := mq.Consumer(name)
	if err != nil {
		return err
	}

	consumer.Consume(handler)

	return nil
}

// Consumer returns a consumer by its name or error if consumer wasn't found.
func (mq *MessageQueue) Consumer(name string) (Consumer, error) {
	consumer, ok := mq.consumers.Get(name)
	if !ok {
		err := fmt.Errorf("consumer '%s' is not registered. Check your configuration", name)

		return nil, err
	}

	return consumer, nil
}

// AsyncProducer returns an async producer by its name or error if producer wasn't found.
// Should be used in most cases.
func (mq *MessageQueue) AsyncProducer(name string) (AsyncProducer, error) {
	item, exists := mq.producers.Get(name)
	producer, asserted := item.(*asyncProducer)

	if !exists || !asserted {
		return nil, fmt.Errorf("producer '%s' is not registered. Check your configuration", name)
	}

	return producer, nil
}

// SyncProducer returns a sync producer by its name or error if producer wasn't found.
func (mq *MessageQueue) SyncProducer(name string) (SyncProducer, error) {
	item, exists := mq.producers.Get(name)
	producer, asserted := item.(*syncProducer)

	if !exists || !asserted {
		return nil, fmt.Errorf("producer '%s' is not registered. Check your configuration", name)
	}

	return producer, nil
}

// Error returns channel with all occurred errors.
// Errors from sync producer won't be accessible. Get them directly from producer.
func (mq *MessageQueue) Error() <-chan error {
	return mq.errorChannel
}

// Close stops all consumers and producers and closes connections to the broker.
func (mq *MessageQueue) Close() {
	mq.stopProducersAndConsumers()

	if mq.channel != nil {
		_ = mq.channel.Close()
	}

	if mq.connection != nil {
		_ = mq.connection.Close()
	}
}

// ConnectionState shows connection state.
func (mq *MessageQueue) ConnectionState() ConnectionState {
	return ConnectionState(atomic.LoadInt32(mq.state))
}

func (mq *MessageQueue) connect() error {
	atomic.StoreInt32(mq.state, int32(ConnectionStateConnecting))
	connection, err := mq.createConnection()
	if err != nil {
		atomic.StoreInt32(mq.state, int32(ConnectionStateDisconnected))
		return err
	}

	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()

		atomic.StoreInt32(mq.state, int32(ConnectionStateDisconnected))
		return err
	}

	mq.connection = connection
	mq.channel = channel

	go mq.handleCloseEvent()

	atomic.StoreInt32(mq.state, int32(ConnectionStateConnected))
	return nil
}

func (mq *MessageQueue) createConnection() (conn, error) {
	mq.cluster.Do(func() { atomic.StoreInt32(&mq.cluster.currentNode, -1) })
	atomic.AddInt32(&mq.cluster.currentNode, 1)
	if int(mq.cluster.currentNode) >= len(mq.config.dsnList) {
		atomic.StoreInt32(&mq.cluster.currentNode, 0)
	}
	dsn := mq.config.dsnList[mq.cluster.currentNode]

	if brokerIsMocked || mq.config.TestMode {
		return amqptest.Dial(dsn)
	}

	return amqp.Dial(dsn)
}

// Register close handler.
// To get more details visit https://godoc.org/github.com/streadway/amqp#Connection.NotifyClose.
func (mq *MessageQueue) handleCloseEvent() {
	err := <-mq.connection.NotifyClose(make(chan wabbit.Error))
	if err != nil {
		mq.internalErrorChannel <- err
	}
	atomic.StoreInt32(mq.state, int32(ConnectionStateDisconnected))
}

func (mq *MessageQueue) errorHandler() {
	for err := range mq.internalErrorChannel {
		select {
		case mq.errorChannel <- err: // Proxies errors to the user.
		default: // For those clients who don't read errors.
		}

		mq.processError(err)
	}
}

func (mq *MessageQueue) processError(err error) {
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
		// Wabbit error. Means that server is down.
		// Used in tests.
		if err.Error() == "Network unreachable" {
			go mq.reconnect()
		}
	}
}

func (mq *MessageQueue) initialSetup() error {
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
func (mq *MessageQueue) setupAfterReconnect() error {
	if err := mq.setupExchanges(); err != nil {
		return err
	}

	if err := mq.setupQueues(); err != nil {
		return err
	}

	mq.producers.GoEach(func(producer internalProducer) {
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

func (mq *MessageQueue) setupExchanges() error {
	for _, config := range mq.config.Exchanges {
		if err := mq.declareExchange(config); err != nil {
			return err
		}
	}

	return nil
}

func (mq *MessageQueue) declareExchange(config ExchangeConfig) error {
	return mq.channel.ExchangeDeclare(config.Name, config.Type, wabbit.Option(config.Options))
}

func (mq *MessageQueue) setupQueues() error {
	for _, config := range mq.config.Queues {
		if err := mq.declareQueue(config); err != nil {
			return err
		}
	}

	return nil
}

func (mq *MessageQueue) declareQueue(config QueueConfig) error {
	if _, err := mq.channel.QueueDeclare(config.Name, wabbit.Option(config.Options)); err != nil {
		return err
	}

	return mq.channel.QueueBind(config.Name, config.RoutingKey, config.Exchange, wabbit.Option(config.BindingOptions))
}

func (mq *MessageQueue) setupProducers() error {
	for _, config := range mq.config.Producers {
		if err := mq.registerProducer(config); err != nil {
			return err
		}
	}

	return nil
}

func (mq *MessageQueue) registerProducer(config ProducerConfig) error {
	if _, ok := mq.producers.Get(config.Name); ok {
		return fmt.Errorf(`producer with name "%s" is already registered`, config.Name)
	}

	channel, err := mq.connection.Channel()
	if err != nil {
		return err
	}

	producer := newInternalProducer(channel, mq.internalErrorChannel, config)
	producer.init()
	mq.producers.Set(config.Name, producer)

	return nil
}

func (mq *MessageQueue) reconnectProducer(producer internalProducer) error {
	channel, err := mq.connection.Channel()
	if err != nil {
		return err
	}

	producer.setChannel(channel)
	producer.init()

	return nil
}

func (mq *MessageQueue) setupConsumers() error {
	for _, config := range mq.config.Consumers {
		if err := mq.registerConsumer(config); err != nil {
			return err
		}
	}

	return nil
}

func (mq *MessageQueue) registerConsumer(config ConsumerConfig) error {
	if _, ok := mq.consumers.Get(config.Name); ok {
		return fmt.Errorf(`consumer with name "%s" is already registered`, config.Name)
	}

	// Consumer must have at least one worker.
	if config.Workers < 1 {
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

func (mq *MessageQueue) reconnectConsumer(consumer *consumer) error {
	for _, worker := range consumer.workers {
		if err := mq.initializeConsumersWorker(consumer, worker); err != nil {
			return err
		}
	}

	consumer.consume(consumer.handler)

	return nil
}

func (mq *MessageQueue) initializeConsumersWorker(consumer *consumer, worker *worker) error {
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
func (mq *MessageQueue) reconnect() {
	startedReconnect := atomic.CompareAndSwapInt32(&mq.reconnectStatus, statusReadyForReconnect, statusReconnecting)
	// There is no need to start a new reconnect if the previous one is not finished yet.
	if !startedReconnect {
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

func (mq *MessageQueue) stopProducersAndConsumers() {
	mq.producers.GoEach(func(producer internalProducer) {
		producer.Stop()
	})

	mq.consumers.GoEach(func(consumer *consumer) {
		consumer.Stop()
	})
}
