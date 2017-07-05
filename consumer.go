package mq

import (
	"sync"
	"sync/atomic"

	"github.com/NeowayLabs/wabbit"
)

type Consumer interface {
	Consume(handler ConsumerHandler)
}

type ConsumerHandler func(message Message)

type Message interface {
	Ack(multiple bool) error
	Nack(multiple, request bool) error
	Reject(requeue bool) error
	Body() []byte
}

type consumer struct {
	handler ConsumerHandler
	once    sync.Once
	workers []worker

	// Reconnect options.
	queue   string
	name    string
	options wabbit.Option
}

func newConsumer(config ConsumerConfig) *consumer {
	return &consumer{
		workers: make([]worker, config.Workers),
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

// TODO Add wait group.
func (consumer *consumer) Stop() {
	for _, worker := range consumer.workers {
		worker.Stop()
	}
}

type worker struct {
	deliveries      <-chan wabbit.Delivery
	shutdownChannel chan struct{}
	status          int32
}

func newWorker(deliveries <-chan wabbit.Delivery) worker {
	return worker{
		deliveries:      deliveries,
		shutdownChannel: make(chan struct{}),
	}
}

func (worker worker) Run(handler ConsumerHandler) {
	atomic.StoreInt32(&worker.status, statusRunning)

	for {
		select {
		case message := <-worker.deliveries:
			if message == nil { // Channel has been closed.
				return
			}

			handler(message)
		case <-worker.shutdownChannel:
			return
		}
	}
}

// Force stop.
// TODO Add wait group.
func (worker worker) Stop() {
	needsToShutdown := atomic.CompareAndSwapInt32(&worker.status, statusRunning, statusStopped)
	if needsToShutdown {
		worker.shutdownChannel <- struct{}{}
	}
}
