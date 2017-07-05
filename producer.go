package mq

import (
	"sync"
	"sync/atomic"

	"github.com/NeowayLabs/wabbit"
)

// Producer describes available methods for producer.
type Producer interface {
	Produce(data []byte)
}

type producer struct {
	sync.RWMutex    // Protect channel during posting and reconnect.
	channel         wabbit.Channel
	errorChannel    chan<- error
	exchange        string
	options         wabbit.Option
	publishChannel  chan []byte
	resendMutex     sync.Mutex
	resendChannel   chan []byte
	routingKey      string
	shutdownChannel chan struct{}
	status          int32 // Defines worker state: running or stopped.
}

func newProducer(channel wabbit.Channel, errorChannel chan<- error, config ProducerConfig) *producer {
	return &producer{
		channel:         channel,
		errorChannel:    errorChannel,
		exchange:        config.Exchange,
		options:         wabbit.Option(config.Options),
		publishChannel:  make(chan []byte, config.BufferSize),
		resendChannel:   make(chan []byte, 1),
		routingKey:      config.RoutingKey,
		shutdownChannel: make(chan struct{}),
	}
}

// Method safely sets new RMQ channel.
func (producer *producer) setChannel(channel wabbit.Channel) {
	producer.Lock()
	producer.channel = channel
	producer.Unlock()
}

func (producer *producer) worker() {
	atomic.StoreInt32(&producer.status, statusRunning)

	for {
		select {
		case message := <-producer.publishChannel:
			err := producer.produce(message)
			if err != nil {
				producer.errorChannel <- err

				// TODO make it optional.
				// TODO check error types. Why the message is broken?
				producer.resendMutex.Lock()
				producer.resendChannel <- message
			}
		case message := <-producer.resendChannel:
			err := producer.produce(message)
			if err != nil {
				producer.errorChannel <- err
				producer.resendChannel <- message
			} else {
				producer.resendMutex.Unlock()
			}
		case <-producer.shutdownChannel:
			// TODO It is necessary to guarantee the message delivery order.
			return
		}
	}
}

func (producer *producer) Produce(message []byte) {
	producer.publishChannel <- message
}

func (producer *producer) produce(message []byte) error {
	producer.RLock()
	defer producer.RUnlock()

	return producer.channel.Publish(producer.exchange, producer.routingKey, message, producer.options)
}

// Stops the worker if it is running.
// TODO Add wait group.
func (producer *producer) Stop() {
	needsToShutdown := atomic.CompareAndSwapInt32(&producer.status, statusRunning, statusStopped)
	if needsToShutdown {
		producer.shutdownChannel <- struct{}{}
	}
}
