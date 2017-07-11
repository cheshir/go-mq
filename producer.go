package mq

import (
	"sync"

	"github.com/NeowayLabs/wabbit"
)

// Producer describes available methods for producer.
type Producer interface {
	Produce(data []byte)
}

type producer struct {
	sync.Mutex // Protect channel during posting and reconnect.
	workerStatus

	channel         wabbit.Channel
	errorChannel    chan<- error
	exchange        string
	options         wabbit.Option
	publishChannel  chan []byte
	resendMutex     sync.Mutex
	resendChannel   chan []byte
	routingKey      string
	shutdownChannel chan struct{}
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

func (producer *producer) worker() {
	producer.markAsRunning()

	for {
		select {
		case message := <-producer.publishChannel:
			err := producer.produce(message)
			if err != nil {
				producer.errorChannel <- err
				// TODO Resend message.
			}
		case <-producer.shutdownChannel:
			// TODO It is necessary to guarantee the message delivery order.
			producer.closeChannel()

			return
		}
	}
}

// Method safely sets new RMQ channel.
func (producer *producer) setChannel(channel wabbit.Channel) {
	producer.Lock()
	producer.channel = channel
	producer.Unlock()
}

// Close producer's channel.
func (producer *producer) closeChannel() {
	producer.Lock()
	if err := producer.channel.Close(); err != nil {
		producer.errorChannel <- err
	}
	producer.Unlock()
}

func (producer *producer) Produce(message []byte) {
	producer.publishChannel <- message
}

func (producer *producer) produce(message []byte) error {
	producer.Lock()
	defer producer.Unlock()

	return producer.channel.Publish(producer.exchange, producer.routingKey, message, producer.options)
}

// Stops the worker if it is running.
// TODO Add wait group.
func (producer *producer) Stop() {
	if producer.markAsStoppedIfCan() {
		producer.shutdownChannel <- struct{}{}
	}
}
