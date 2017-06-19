package mq

import (
	"sync"

	"github.com/NeowayLabs/wabbit"
)

type Producer interface {
	Produce(data []byte)
}

type producer struct {
	sync.RWMutex   // Protect channel during reconnect.
	channel        wabbit.Channel
	errorChannel   chan error
	exchange       string
	options        wabbit.Option
	publishChannel chan []byte
	routingKey     string
}

// Method safely sets new RMQ channel.
func (producer *producer) setChannel(channel wabbit.Channel) {
	producer.Lock()
	producer.channel = channel
	producer.Unlock()
}

func (producer *producer) worker() {
	for message := range producer.publishChannel {
		if err := producer.produce(message); err != nil {
			producer.errorChannel <- err
		}
	}
}

func (producer *producer) Produce(data []byte) {
	producer.publishChannel <- data
}

func (producer *producer) produce(message []byte) error {
	producer.RLock()
	defer producer.RUnlock()

	return producer.channel.Publish(producer.exchange, producer.routingKey, message, producer.options)
}
