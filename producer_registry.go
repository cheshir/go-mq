package mq

import (
	"sync"
)

func newProducersRegistry(size int) *producersRegistry {
	return &producersRegistry{
		producers: make(map[string]internalProducer, size),
	}
}

type producersRegistry struct {
	sync.Mutex
	producers map[string]internalProducer
}

func (registry *producersRegistry) Get(name string) (internalProducer, bool) {
	registry.Lock()
	producer, ok := registry.producers[name]
	registry.Unlock()

	return producer, ok
}

func (registry *producersRegistry) Set(name string, producer internalProducer) {
	registry.Lock()
	registry.producers[name] = producer
	registry.Unlock()
}

func (registry *producersRegistry) GoEach(fn func(internalProducer)) {
	wg := &sync.WaitGroup{}

	registry.Lock()
	defer registry.Unlock()

	wg.Add(len(registry.producers))

	for _, p := range registry.producers {
		go func(producer internalProducer) {
			fn(producer)
			wg.Done()
		}(p)
	}

	wg.Wait()
}
