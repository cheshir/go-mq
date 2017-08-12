package mq

import (
	"sync"

	"github.com/NeowayLabs/wabbit"
)

// Consumer describes available methods for consumer.
type Consumer interface {
	Consume(handler ConsumerHandler)
}

// ConsumerHandler describes handler function signature.
// It will be called for each obtained message.
type ConsumerHandler func(message Message)

// Message describes available methods of the message obtained from queue.
type Message interface {
	Ack(multiple bool) error
	Nack(multiple, request bool) error
	Reject(requeue bool) error
	Body() []byte
}

type consumer struct {
	handler ConsumerHandler
	once    sync.Once
	workers []*worker

	// Options for reconnect.
	queue         string
	name          string
	options       wabbit.Option
	prefetchCount int
	prefetchSize  int
}

func newConsumer(config ConsumerConfig) *consumer {
	return &consumer{
		workers: make([]*worker, config.Workers),
		queue:   config.Queue,
		name:    config.Name,
		options: wabbit.Option(config.Options),
	}
}

// Consume sets handler for incoming messages and runs it.
// Can be called only once.
func (consumer *consumer) Consume(handler ConsumerHandler) {
	consumer.once.Do(func() {
		consumer.handler = handler

		for _, worker := range consumer.workers {
			go worker.Run(handler)
		}
	})
}

// Stop terminates consumer's workers.
func (consumer *consumer) Stop() {
	for _, worker := range consumer.workers {
		worker.Stop()
	}
}

type worker struct {
	sync.Mutex // Protect channel during reconnect.
	workerStatus

	channel         wabbit.Channel
	deliveries      <-chan wabbit.Delivery
	errorChannel    chan<- error
	shutdownChannel chan struct{}
}

func newWorker(errorChannel chan<- error) *worker {
	return &worker{
		errorChannel:    errorChannel,
		shutdownChannel: make(chan struct{}),
	}
}

func (worker *worker) Run(handler ConsumerHandler) {
	worker.markAsRunning()

	for {
		select {
		case message := <-worker.deliveries:
			if message == nil { // It seems like channel was closed.
				if worker.markAsStoppedIfCan() {
					// Stop the worker.

					return
				}

				// Somebody is already trying to stop the worker.
				continue
			}

			handler(message)
		case <-worker.shutdownChannel:
			worker.closeChannel()

			return
		}
	}
}

// Method safely sets new RMQ channel.
func (worker *worker) setChannel(channel wabbit.Channel) {
	worker.Lock()
	worker.channel = channel
	worker.Unlock()
}

// Close worker's channel.
func (worker *worker) closeChannel() {
	worker.Lock()
	if err := worker.channel.Close(); err != nil {
		worker.errorChannel <- err
	}
	worker.Unlock()
}

// Force stop.
// TODO Add wait group.
func (worker *worker) Stop() {
	if worker.markAsStoppedIfCan() {
		worker.shutdownChannel <- struct{}{}
	}
}
