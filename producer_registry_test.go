package mq

import (
	"fmt"
	"sync/atomic"
	"testing"
)

func TestProducersRegistry_Get(t *testing.T) {
	cases := []struct {
		Name             string
		ExpectedProducer internalProducer
	}{
		{Name: "Get from async producer", ExpectedProducer: &asyncProducer{}},
		{Name: "Get from sync producer", ExpectedProducer: &syncProducer{}},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			registry := newProducersRegistry(1)
			registry.Set("name", tc.ExpectedProducer)
			actualProducer, ok := registry.Get("name")

			if !ok {
				t.Error("Expected producer was not found")
			}

			if tc.ExpectedProducer != actualProducer {
				t.Errorf("Expected and actual producers are not eqaul")
			}
		})
	}
}

func TestProducersRegistry_Get_NonExistent(t *testing.T) {
	registry := newProducersRegistry(1)
	producer, ok := registry.Get("name")

	if ok || producer != nil {
		t.Error("Registry found a non-registered producer")
	}
}

func TestProducersRegistry_Concurrent(t *testing.T) {
	size := 1000
	registry := newProducersRegistry(size)

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

func TestProducersRegistry_GoEach(t *testing.T) {
	registry := newProducersRegistry(1)
	registry.Set("1", &asyncProducer{})
	registry.Set("2", &asyncProducer{})
	registry.Set("3", &syncProducer{})
	registry.Set("4", &syncProducer{})

	var counter int32

	registry.GoEach(func(producer internalProducer) {
		atomic.AddInt32(&counter, 1)
	})

	if atomic.LoadInt32(&counter) != 4 {
		t.Error("Go each must wait until all functions will finish")
	}
}
