package mq

import (
	"sync/atomic"
	"testing"
)

func TestProducersRegistry_Get(t *testing.T) {
	expectedProducer := &producer{}

	registry := newProducersRegistry(1)
	registry.Set("name", expectedProducer)
	actualProducer, ok := registry.Get("name")

	if !ok {
		t.Error("Expected producer was not found")
	}

	if expectedProducer != actualProducer {
		t.Errorf("Expected and actual producers are not eqaul")
	}
}

func TestProducersRegistry_Get_NonExistent(t *testing.T) {
	registry := newProducersRegistry(1)
	producer, ok := registry.Get("name")

	if ok || producer != nil {
		t.Error("Registry found a non-registered producer")
	}
}

func TestProducersRegistry_Get_Parallel(t *testing.T) {
	registry := newProducersRegistry(1)
	registry.Set("name", nil)

	t.Run("concurrent write", func(t *testing.T) {
		t.Parallel()
		registry.Set("name 2", nil)
	})
}

func TestProducersRegistry_GoEach(t *testing.T) {
	registry := newProducersRegistry(1)
	registry.Set("1", &producer{})
	registry.Set("2", &producer{})
	registry.Set("3", &producer{})
	registry.Set("4", &producer{})

	var counter int32

	registry.GoEach(func(producer *producer) {
		atomic.AddInt32(&counter, 1)
	})

	if atomic.LoadInt32(&counter) != 4 {
		t.Error("Go each must wait until all functions will finish")
	}
}

func TestConsumersRegistry_Get(t *testing.T) {
	expectedConsumer := &consumer{}

	registry := newConsumersRegistry(1)
	registry.Set("name", expectedConsumer)
	actualConsumer, ok := registry.Get("name")

	if !ok {
		t.Error("Expected consumer was not found")
	}

	if expectedConsumer != actualConsumer {
		t.Errorf("Expected and actual consumers are not eqaul")
	}
}

func TestConsumersRegistry_Get_NonExistent(t *testing.T) {
	registry := newConsumersRegistry(1)
	consumer, ok := registry.Get("name")

	if ok || consumer != nil {
		t.Error("Registry found a non-registered consumer")
	}
}

func TestConsumersRegistry_Get_Parallel(t *testing.T) {
	registry := newConsumersRegistry(1)
	registry.Set("name", nil)

	t.Run("concurrent write", func(t *testing.T) {
		t.Parallel()
		registry.Set("name 2", nil)
	})
}

func TestConsumersRegistry_GoEach(t *testing.T) {
	registry := newConsumersRegistry(1)
	registry.Set("1", &consumer{})
	registry.Set("2", &consumer{})
	registry.Set("3", &consumer{})
	registry.Set("4", &consumer{})

	var counter int32

	registry.GoEach(func(consumer *consumer) {
		atomic.AddInt32(&counter, 1)
	})

	if atomic.LoadInt32(&counter) != 4 {
		t.Error("Go each must wait until all functions will finish")
	}
}
