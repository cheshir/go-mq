package mq

import (
	"sync"
)

func newConsumersRegistry(size int) *consumersRegistry {
	return &consumersRegistry{
		consumers: make(map[string]*consumer, size),
	}
}

type consumersRegistry struct {
	sync.Mutex
	consumers map[string]*consumer
}

func (registry *consumersRegistry) Get(name string) (*consumer, bool) {
	registry.Lock()
	consumer, ok := registry.consumers[name]
	registry.Unlock()

	return consumer, ok
}

func (registry *consumersRegistry) Set(name string, consumer *consumer) {
	registry.Lock()
	registry.consumers[name] = consumer
	registry.Unlock()
}

func (registry *consumersRegistry) GoEach(fn func(*consumer)) {
	wg := &sync.WaitGroup{}

	registry.Lock()
	defer registry.Unlock()

	wg.Add(len(registry.consumers))

	for _, c := range registry.consumers {
		go func(consumer *consumer) {
			fn(consumer)
			wg.Done()
		}(c)
	}

	wg.Wait()
}
