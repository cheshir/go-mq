package mq

import (
	"fmt"
	"sync/atomic"
	"testing"
)

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

func TestConsumersRegistry_Concurrent(t *testing.T) {
	size := 1000
	registry := newConsumersRegistry(size)

	t.Run("concurrent write", func(t *testing.T) {
		t.Parallel()
		for i := 0; i < size; i++ {
			registry.Set(fmt.Sprintf("name-%d", i), nil)
		}
	})

	t.Run("concurrent read", func(t *testing.T) {
		t.Parallel()
		for i := 0; i < size; i++ {
			registry.Get(fmt.Sprintf("name-%d", i))
		}
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
