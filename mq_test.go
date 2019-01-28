package mq

import (
	"encoding/json"
	"flag"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NeowayLabs/wabbit/amqptest/server"
	"gopkg.in/yaml.v1"
)

const (
	dsnForTests = "amqp://guest:guest@localhost:5672/"

	defaultConsumerName      = "default_consumer"
	defaultExchangeName      = "default_exchange"
	defaultExchangeType      = "direct"
	defaultQueueName         = "default_queue"
	defaultAsyncProducerName = "default_async_producer"
	defaultSyncProducerName  = "default_sync_producer"
	defaultRoutingKey        = "routing_key"
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

	cases := []struct {
		Name   string
		Config func() (Config, error)
	}{
		{
			"with internal struct config",
			func() (Config, error) {
				return newDefaultConfig(), nil
			},
		},
		{
			"with json config",
			func() (Config, error) {
				marshaled, err := json.Marshal(newDefaultConfig())
				if err != nil {
					t.Fatalf("Failed to marshal config to JSON: %v", err)
				}

				config := Config{}
				if err = json.Unmarshal(marshaled, &config); err != nil {
					t.Fatalf("Failed to unmarshal test data: %v", err)
				}

				return config, nil
			},
		},
		{
			"with yaml config",
			func() (Config, error) {
				marshaled, err := yaml.Marshal(newDefaultConfig())
				if err != nil {
					t.Fatalf("Failed to marshal config to yaml: %v", err)
				}

				config := Config{}
				if err = yaml.Unmarshal(marshaled, &config); err != nil {
					t.Fatalf("Failed to unmarshal test data: %v", err)
				}

				return config, err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			config, err := tc.Config()
			if err != nil {
				t.Fatalf("Failed to get config for test: %v", err)
			}

			mq, err := New(config)
			if err != nil {
				t.Fatalf("Failed to create a new instance of mq: %v", err)
			}
			defer mq.Close()

			done := make(chan struct{})
			go func() {
				for {
					select {
					case err := <-mq.Error():
						t.Errorf("unexpected error from queue: %v", err)
					case <-done:
						return
					}
				}
			}()

			expectedMessage := []byte("test")

			asyncProducer, err := mq.AsyncProducer(defaultAsyncProducerName)
			if err != nil {
				t.Fatal("Failed to get registered producer")
			}

			asyncProducer.Produce(expectedMessage)

			syncProducer, err := mq.SyncProducer(defaultSyncProducerName)
			if err != nil {
				t.Fatal("Failed to get registered producer")
			}

			err = syncProducer.Produce(expectedMessage)
			if err != nil {
				t.Fatal("Failed to produce message")
			}

			var messageWasRead int32
			mq.SetConsumerHandler(defaultConsumerName, func(message Message) {
				atomic.AddInt32(&messageWasRead, 1)

				if !isSliceOfBytesIsEqual(expectedMessage, message.Body()) {
					message.Reject(false)
					t.Errorf("Actual message '%s' is not equal to expected '%s'", message.Body(), expectedMessage)
				}

				message.Ack(false)
			})

			waitForMessageDelivery()

			readMessages := atomic.LoadInt32(&messageWasRead)
			if readMessages != 2 {
				t.Errorf("Consumer did not read messages. Produced %d, read %d", 2, readMessages)
			}

			assertNoMqError(t, mq)
			done <- struct{}{}
		})
	}
}

func newDefaultConfig() Config {
	return Config{
		DSN: dsnForTests,
		Consumers: Consumers{{
			Name:          defaultConsumerName,
			Queue:         defaultQueueName,
			PrefetchCount: 1,
			Workers:       4,
			Options: Options{
				"no_local": false, // Just for example.
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
				"args": map[string]interface{}{
					"x-max-priority": 9,
				},
			},
		}},
		Producers: Producers{
			{
				Name:       defaultAsyncProducerName,
				Exchange:   defaultExchangeName,
				RoutingKey: defaultRoutingKey,
				Options: Options{
					"delivery_mode": 1,
				},
			},
			{
				Name:       defaultSyncProducerName,
				Exchange:   defaultExchangeName,
				RoutingKey: defaultRoutingKey,
				Sync:       true,
				Options: Options{
					"delivery_mode": 1,
				},
			},
		},
	}
}

func TestMq_Reconnect(t *testing.T) {
	if !brokerIsMocked {
		t.Skip("We can't stop real broker from test.")
	}

	broker := server.NewServer(dsnForTests)
	broker.Start()

	messageQueue, err := New(Config{
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
		Producers: Producers{
			{
				BufferSize: 1,
				Name:       defaultAsyncProducerName,
				Exchange:   defaultExchangeName,
				RoutingKey: defaultRoutingKey,
			},
			{
				Name:       defaultSyncProducerName,
				Exchange:   defaultExchangeName,
				RoutingKey: defaultRoutingKey,
				Sync:       true,
			},
		},
	})

	if err != nil {
		t.Fatal("Can't create a new instance of mq: ", err)
	}

	expectedMessage := []byte("test")

	var messageWasRead int32
	messageQueue.SetConsumerHandler(defaultConsumerName, func(message Message) {
		atomic.AddInt32(&messageWasRead, 1)

		if !isSliceOfBytesIsEqual(expectedMessage, message.Body()) {
			message.Reject(false)
			t.Errorf("Actual message '%s' is not equal to expected '%s'", message.Body(), expectedMessage)
		}

		message.Ack(false)
	})

	broker.Stop() // Force reconnect.

	asyncProducer, err := messageQueue.AsyncProducer(defaultAsyncProducerName)
	if err != nil {
		t.Error("Failed to get registered producer")
	}

	asyncProducer.Produce(expectedMessage)

	syncProducer, err := messageQueue.SyncProducer(defaultSyncProducerName)
	if err != nil {
		t.Error("Failed to get registered producer")
	}

	syncProducer.Produce(expectedMessage)

	broker.Start()
	defer broker.Stop()

	waitForMessageDelivery()

	readMessages := atomic.LoadInt32(&messageWasRead)
	if readMessages != 2 {
		// Probably known problem. Check https://github.com/cheshir/go-mq/issues/25 for details.
		t.Errorf("Consumer did not read messages. Produced %d, read %d", 2, readMessages)
	}

	messageQueue.Close()
}

func TestMq_Consumer_Exists(t *testing.T) {
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
			Options: Options{
				"args": map[interface{}]interface{}{
					"x-max-priority": byte(9),
				},
			},
		}},
	})

	if err != nil {
		t.Error("Can't create a new instance of mq: ", err)
	}

	defer mq.Close()

	_, err = mq.Consumer(defaultConsumerName)
	if err != nil {
		t.Error("Failed to get a consumer: ", err)
	}

	assertNoMqError(t, mq)
}

func TestMq_Consumer_NotExists(t *testing.T) {
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

	_, err = mq.Consumer("nonexistent_consumer")
	if err == nil {
		t.Error("Did not catch an error during the retrieval of non-existent consumer.")
	}

	assertNoMqError(t, mq)
}

func TestMq_Producer_Exists(t *testing.T) {
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
			Options: Options{
				"args": map[interface{}]interface{}{
					"x-max-priority": byte(9),
				},
			},
		}},
		Producers: Producers{
			{
				Name:     defaultAsyncProducerName,
				Exchange: defaultExchangeName,
			},
			{
				Name:     defaultSyncProducerName,
				Exchange: defaultExchangeName,
				Sync:     true,
			},
		},
	})

	if err != nil {
		t.Error("Can't create a new instance of mq: ", err)
	}

	defer mq.Close()

	_, err = mq.AsyncProducer(defaultAsyncProducerName)
	if err != nil {
		t.Error("Failed to get an async producer: ", err)
	}

	_, err = mq.SyncProducer(defaultSyncProducerName)
	if err != nil {
		t.Error("Failed to get a sync producer: ", err)
	}

	assertNoMqError(t, mq)
}

func TestMq_Producer_NonExistent(t *testing.T) {
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

	_, err = mq.AsyncProducer("nonexistent_producer")
	if err == nil {
		t.Error("No error got during getting non existent async producer")
	}

	_, err = mq.SyncProducer("nonexistent_producer")
	if err == nil {
		t.Error("No error got during getting non existent sync producer")
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
	time.Sleep(300 * time.Millisecond)
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
