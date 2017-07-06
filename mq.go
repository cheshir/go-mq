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
	amqpDriver "github.com/streadway/amqp"
)

const (
	// Describes states during reconnect.
	statusReadyForReconnect int32 = 0
	statusReconnecting            = 1

	// Describes worker states.
	statusStopped = 0
	statusRunning = 1

	// Qos options.
	// TODO Move them to config.
	prefetchCount = 1
	prefetchSize  = 0
)

// New initializes AMQP connection to the message broker
// and returns adapter that provides an ability
// to get configured consumers and producers, read occurred errors and shutdown all workers.
func New(config Config) (MQ, error) {
	config.normalize()

	mq := &mq{
		errorChannel:         make(chan error),
		dsn:                  config.DSN,
		internalErrorChannel: make(chan error),
		consumers:            newConsumersRegistry(len(config.Consumers)),
		producers:            newProducersRegistry(len(config.Producers)),
		reconnectDelay:       config.ReconnectDelay,
	}

	if err := mq.connect(); err != nil {
		return nil, err
	}

	go mq.errorHandler()

	return mq, mq.setup(config)
}

// MQ describes methods provided by message broker adapter.
type MQ interface {
	GetConsumer(name string) (Consumer, error)
	SetConsumerHandler(name string, handler ConsumerHandler) error
	GetProducer(name string) (Producer, error)
	Error() <-chan error
	Close()
}

type mq struct {
	channel              wabbit.Channel
	connection           wabbit.Conn
	errorChannel         chan error
	dsn                  string // We need to store it for reconnect.
	internalErrorChannel chan error
	consumers            *consumersRegistry
	producers            *producersRegistry
	reconnectStatus      int32         // Defines whether client is trying to reconnect or not.
	reconnectDelay       time.Duration // Delay before reconnect in seconds.
}

func (mq *mq) setup(config Config) error {
	if err := mq.setupExchanges(config.Exchanges); err != nil {
		return err
	}

	if err := mq.setupQueues(config.Queues); err != nil {
		return err
	}

	if err := mq.setupProducers(config.Producers); err != nil {
		return err
	}

	if err := mq.setupConsumers(config.Consumers); err != nil {
		return err
	}

	return nil
}

func (mq *mq) setupExchanges(exchanges Exchanges) error {
	for _, config := range exchanges {
		if err := mq.declareExchange(config); err != nil {
			return err
		}
	}

	return nil
}

func (mq *mq) declareExchange(config ExchangeConfig) error {
	return mq.channel.ExchangeDeclare(config.Name, config.Type, wabbit.Option(config.Options))
}

func (mq *mq) setupQueues(queues Queues) error {
	for _, config := range queues {
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

func (mq *mq) setupProducers(producers Producers) error {
	for _, config := range producers {
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

	producer := newProducer(mq.channel, mq.internalErrorChannel, config)

	go producer.worker()
	mq.producers.Set(config.Name, producer)

	return nil
}

func (mq *mq) setupConsumers(consumers Consumers) error {
	for _, config := range consumers {
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

	for i := 0; i < config.Workers; i++ {
		deliveries, err := mq.channel.Consume(consumer.queue, "", consumer.options)
		if err != nil {
			return fmt.Errorf(`Error during the consumer starting %s: %s`, config.Name, err)
		}

		consumer.workers[i] = newWorker(deliveries)
	}

	mq.consumers.Set(config.Name, consumer) // Workers will start after consumer.Consume method call.

	return nil
}

func (mq *mq) reconnectConsumer(consumer *consumer) error {
	for _, worker := range consumer.workers {
		deliveries, err := mq.channel.Consume(consumer.queue, "", consumer.options)
		if err != nil {
			return err
		}

		worker.deliveries = deliveries
		go worker.Run(consumer.handler)
	}

	return nil
}

func (mq *mq) connect() error {
	connection, err := amqp.Dial(mq.dsn)
	if err != nil {
		return err
	}

	channel, err := connection.Channel()
	if err != nil {
		connection.Close()

		return err
	}

	if err := channel.Qos(prefetchCount, prefetchSize, false); err != nil {
		channel.Close()
		connection.Close()

		return err
	}

	mq.connection = connection
	mq.channel = channel

	go mq.handleCloseEvent()

	return nil
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
		select {
		case mq.errorChannel <- err: // Proxies errors to the user.
		default: // Drop errors if channel buffer is full.
			// TODO It probably makes sense to make it optional or even remove.
		}

		mq.processError(err)
	}
}

func (mq *mq) processError(err interface{}) {
	switch err.(type) {
	case *net.OpError:
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

	mq.stopWorkers()

	time.Sleep(mq.reconnectDelay) // TODO Add incremental sleep.

	if err := mq.connect(); err != nil {
		mq.internalErrorChannel <- err

		return
	}

	mq.producers.GoEach(func(producer *producer) {
		producer.setChannel(mq.channel)
		go producer.worker()
	})

	mq.consumers.GoEach(func(consumer *consumer) {
		if err := mq.reconnectConsumer(consumer); err != nil {
			mq.internalErrorChannel <- err
		}
	})
}

func (mq *mq) stopWorkers() {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		mq.producers.GoEach(func(producer *producer) {
			producer.Stop()
		})

		wg.Done()
	}()

	go func() {
		mq.consumers.GoEach(func(consumer *consumer) {
			consumer.Stop()
		})

		wg.Done()
	}()

	wg.Wait()
}

// GetConsumer returns a consumer by its name or error if consumer wasn't found.
func (mq *mq) GetConsumer(name string) (consumer Consumer, err error) {
	consumer, ok := mq.consumers.Get(name)
	if !ok {
		err = fmt.Errorf("Conumer '%s' is not registered. Check your configuration.", name)
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
	mq.stopWorkers()

	if mq.channel != nil {
		mq.channel.Close()
	}

	if mq.connection != nil {
		mq.connection.Close()
	}
}
