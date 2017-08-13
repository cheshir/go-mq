package mq

import (
	"flag"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NeowayLabs/wabbit/amqptest/server"
)

const (
	dsnForTests = "amqp://guest:guest@localhost:5672/"

	defaultConsumerName = "default_consumer"
	defaultExchangeName = "default_exchange"
	defaultExchangeType = "direct"
	defaultQueueName    = "default_queue"
	defaultProducerName = "default_producer"
	defaultRoutingKey   = "routing_key"
)

func init() {
	flag.BoolVar(&brokerIsMocked, "mock-broker", true, "Whether to mock broker or to use a real one. 0: use real broker. 1: use mocked broker.")
}

func TestMq_ConnectToStoppedBroker(t *testing.T) {
	if !brokerIsMocked {
		t.Skip("We can't stop real broker from test.")
	}

	_, err := New(Config{
		DSN: dsnForTests,
	})

	if err == nil {
		t.Error("Mq must not successfully connect to the broker when it's stopped.")
	}
}

func TestMq_ProduceConsume(t *testing.T) {
	if brokerIsMocked {
		broker := server.NewServer(dsnForTests)
		broker.Start()
		defer broker.Stop()
	}

	mq, err := New(Config{
		DSN: dsnForTests,
		Consumers: Consumers{{
			Name:          defaultConsumerName,
			Queue:         defaultQueueName,
			PrefetchCount: 1,
			Workers:       4,
			Options: Options{
				"no_local": true, // Just for example
			},
		}},
		Exchanges: Exchanges{{
			Name: defaultExchangeName,
			Type: defaultExchangeType,
			Options: Options{
				"durable": false,
			},
		}},
		Queues: Queues{{
			Name:       defaultQueueName,
			Exchange:   defaultExchangeName,
			RoutingKey: defaultRoutingKey,
			Options: Options{
				"durable": false,
			},
		}},
		Producers: Producers{{
			Name:       defaultProducerName,
			Exchange:   defaultExchangeName,
			RoutingKey: defaultRoutingKey,
			Options: Options{
				"delivery_mode": 1,
			},
		}},
	})

	if err != nil {
		t.Error("Can't create a new instance of mq: ", err)
	}

	defer mq.Close()

	expectedMessage := []byte("test")

	producer, err := mq.GetProducer(defaultProducerName)
	if err != nil {
		t.Error("Failed to get registered producer")
	}

	producer.Produce(expectedMessage)

	var messageWasRead int32
	mq.SetConsumerHandler(defaultConsumerName, func(message Message) {
		atomic.StoreInt32(&messageWasRead, 1)

		if !isSliceOfBytesIsEqual(expectedMessage, message.Body()) {
			t.Errorf("Actual message '%s' is not equal to expected '%s'", message.Body(), expectedMessage)
		}
	})

	waitForMessageDelivery()

	if atomic.LoadInt32(&messageWasRead) != 1 {
		t.Error("Consumer did not read the message")
	}

	assertNoMqError(t, mq)
}

func TestMq_Reconnect(t *testing.T) {
	if !brokerIsMocked {
		t.Skip("We can't stop real broker from test.")
	}

	broker := server.NewServer(dsnForTests)
	broker.Start()
	defer broker.Stop()

	mq, err := New(Config{
		DSN:            dsnForTests,
		ReconnectDelay: time.Nanosecond,
		Consumers: Consumers{{
			Name:          defaultConsumerName,
			Queue:         defaultQueueName,
			PrefetchCount: 1,
			Workers:       4,
		}},
		Exchanges: Exchanges{{
			Name: defaultExchangeName,
			Type: defaultExchangeType,
		}},
		Queues: Queues{{
			Name:       defaultQueueName,
			Exchange:   defaultExchangeName,
			RoutingKey: defaultRoutingKey,
		}},
		Producers: Producers{{
			BufferSize: 1,
			Name:       defaultProducerName,
			Exchange:   defaultExchangeName,
			RoutingKey: defaultRoutingKey,
		}},
	})

	if err != nil {
		t.Error("Can't create a new instance of mq: ", err)
	}

	defer mq.Close()

	expectedMessage := []byte("test")

	var messageWasRead int32
	mq.SetConsumerHandler(defaultConsumerName, func(message Message) {
		atomic.StoreInt32(&messageWasRead, 1)

		if !isSliceOfBytesIsEqual(expectedMessage, message.Body()) {
			t.Errorf("Actual message '%s' is not equal to expected '%s'", message.Body(), expectedMessage)
		}
	})

	broker.Stop() // Force reconnect.

	producer, err := mq.GetProducer(defaultProducerName)
	if err != nil {
		t.Error("Failed to get registered producer")
	}

	producer.Produce(expectedMessage)

	broker.Start()
	waitForMessageDelivery()

	if atomic.LoadInt32(&messageWasRead) != 1 {
		t.Error("Consumer did not read the message")
	}
}

func TestMq_GetConsumer_Existent(t *testing.T) {
	if brokerIsMocked {
		broker := server.NewServer(dsnForTests)
		broker.Start()
		defer broker.Stop()
	}

	mq, err := New(Config{
		DSN: dsnForTests,
		Consumers: Consumers{{
			Name:  defaultConsumerName,
			Queue: defaultQueueName,
		}},
		Exchanges: Exchanges{{
			Name: defaultExchangeName,
			Type: defaultExchangeType,
		}},
		Queues: Queues{{
			Name:     defaultQueueName,
			Exchange: defaultExchangeName,
		}},
	})

	if err != nil {
		t.Error("Can't create a new instance of mq: ", err)
	}

	defer mq.Close()

	_, err = mq.GetConsumer(defaultConsumerName)
	if err != nil {
		t.Error("Failed to get a consumer: ", err)
	}

	assertNoMqError(t, mq)
}

func TestMq_GetConsumer_NonExistent(t *testing.T) {
	if brokerIsMocked {
		broker := server.NewServer(dsnForTests)
		broker.Start()
		defer broker.Stop()
	}

	mq, err := New(Config{
		DSN: dsnForTests,
	})

	if err != nil {
		t.Error("Can't create a new instance of mq: ", err)
	}

	defer mq.Close()

	_, err = mq.GetConsumer("nonexistent_consumer")
	if err == nil {
		t.Error("Did not catch an error during the retrieval of non-existent consumer.")
	}

	assertNoMqError(t, mq)
}

func TestMq_GetProducer_Existent(t *testing.T) {
	if brokerIsMocked {
		broker := server.NewServer(dsnForTests)
		broker.Start()
		defer broker.Stop()
	}

	mq, err := New(Config{
		DSN: dsnForTests,
		Exchanges: Exchanges{{
			Name: defaultExchangeName,
			Type: defaultExchangeType,
		}},
		Queues: Queues{{
			Name:     defaultQueueName,
			Exchange: defaultExchangeName,
		}},
		Producers: Producers{{
			Name:     defaultProducerName,
			Exchange: defaultExchangeName,
		}},
	})

	if err != nil {
		t.Error("Can't create a new instance of mq: ", err)
	}

	defer mq.Close()

	_, err = mq.GetProducer(defaultProducerName)
	if err != nil {
		t.Error("Failed to get a producer: ", err)
	}

	assertNoMqError(t, mq)
}

func TestMq_GetProducer_NonExistent(t *testing.T) {
	if brokerIsMocked {
		broker := server.NewServer(dsnForTests)
		broker.Start()
		defer broker.Stop()
	}

	mq, err := New(Config{
		DSN: dsnForTests,
	})

	if err != nil {
		t.Error("Can't create a new instance of mq: ", err)
	}

	defer mq.Close()

	_, err = mq.GetProducer("nonexistent_producer")
	if err == nil {
		t.Error("No error got during getting non existent producer")
	}

	assertNoMqError(t, mq)
}

func TestMq_SetConsumerHandler_NonExistentConsumer(t *testing.T) {
	if brokerIsMocked {
		broker := server.NewServer(dsnForTests)
		broker.Start()
		defer broker.Stop()
	}

	mq, err := New(Config{
		DSN: dsnForTests,
	})

	if err != nil {
		t.Error("Can't create a new instance of mq: ", err)
	}

	defer mq.Close()

	err = mq.SetConsumerHandler("unknown consumer", func(_ Message) {})
	if err == nil {
		t.Error("No error got during setting handler for non existent consumer")
	}

	assertNoMqError(t, mq)
}

func TestMq_New_InvalidQueueConfiguration(t *testing.T) {
	if brokerIsMocked {
		t.Skip("Wabbit can't validate configuration.")
	}

	_, err := New(Config{
		DSN: dsnForTests,
		Queues: Queues{{
			Exchange: "unknown exchange",
		}},
	})

	if err == nil {
		t.Error("New must return error if invalid configuration is provided")
	}
}

func TestMq_New_InvalidExchangeConfiguration(t *testing.T) {
	if brokerIsMocked {
		t.Skip("Wabbit can't validate configuration.")
	}

	_, err := New(Config{
		DSN: dsnForTests,
		Exchanges: Exchanges{{
			Type: "invalid",
		}},
	})

	if err == nil {
		t.Error("New must return error if invalid configuration is provided")
	}
}

func TestMq_New_InvalidConsumerConfiguration(t *testing.T) {
	if brokerIsMocked {
		t.Skip("Wabbit can't validate configuration.")
	}

	_, err := New(Config{
		DSN: dsnForTests,
		Consumers: Consumers{{
			Queue: "unknown queue",
		}},
	})

	if err == nil {
		t.Error("New must return error if invalid configuration is provided")
	}
}

func assertNoMqError(t *testing.T, mq MQ) {
	select {
	case err := <-mq.Error():
		t.Error("Caught an unexpected error from mq: ", err)
	default:
	}
}

func waitForMessageDelivery() {
	time.Sleep(200 * time.Millisecond)
}

func isSliceOfBytesIsEqual(expected, actual []byte) bool {
	if expected == nil && actual == nil {
		return true
	}

	if expected == nil || actual == nil {
		return false
	}

	if len(expected) != len(actual) {
		return false
	}

	for i := range expected {
		if expected[i] != actual[i] {
			return false
		}
	}

	return true
}
