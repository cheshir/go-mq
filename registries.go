package mq

import (
	"sync"

	"github.com/NeowayLabs/wabbit"
)

func newProducersRegistry(size int) producersRegistry {
	return producersRegistry{
		producers: make(map[string]*producer, size),
	}
}

type producersRegistry struct {
	sync.Mutex
	producers map[string]*producer
}

func (registry producersRegistry) Get(name string) (*producer, bool) {
	registry.Lock()
	producer, ok := registry.producers[name]
	registry.Unlock()

	return producer, ok
}

func (registry producersRegistry) Set(name string, producer *producer) {
	registry.Lock()
	registry.producers[name] = producer
	registry.Unlock()
}

func (registry producersRegistry) SetNewChannel(channel wabbit.Channel) {
	registry.Lock()

	for _, producer := range registry.producers {
		producer.setChannel(channel)
	}

	registry.Unlock()
}
