package mq

import "sync"

func newProducersRegistry(size int) *producersRegistry {
	return &producersRegistry{
		producers: make(map[string]*producer, size),
	}
}

type producersRegistry struct {
	sync.Mutex
	producers map[string]*producer
}

func (registry *producersRegistry) Get(name string) (*producer, bool) {
	registry.Lock()
	producer, ok := registry.producers[name]
	registry.Unlock()

	return producer, ok
}

func (registry *producersRegistry) Set(name string, producer *producer) {
	registry.Lock()
	registry.producers[name] = producer
	registry.Unlock()
}

func (registry *producersRegistry) GoEach(fn func(*producer)) {
	wg := &sync.WaitGroup{}

	registry.Lock()
	defer registry.Unlock()

	wg.Add(len(registry.producers))

	for _, p := range registry.producers {
		go func(producer *producer) {
			fn(producer)
			wg.Done()
		}(p)
	}

	wg.Wait()
}

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
