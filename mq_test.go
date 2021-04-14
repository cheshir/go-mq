package mq

import (
	"encoding/json"
	"flag"
	"runtime"
	"strings"
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
		requireNoError(t, broker.Start(), "Failed to start broker")
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
				requireNoError(t, err, "Failed marshal config to json")

				config := Config{}
				err = json.Unmarshal(marshaled, &config)
				requireNoError(t, err, "Failed unmarshal test data")

				return config, nil
			},
		},
		{
			"with yaml config",
			func() (Config, error) {
				marshaled, err := yaml.Marshal(newDefaultConfig())
				requireNoError(t, err, "Failed marshal config to yaml")

				config := Config{}
				err = yaml.Unmarshal(marshaled, &config)
				requireNoError(t, err, "Failed unmarshal test data")

				return config, err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			config, err := tc.Config()
			requireNoError(t, err, "Failed to get config for test")

			mq, err := New(config)
			requireNoError(t, err, "Failed to create a new instance of mq")
			defer mq.Close()

			done := make(chan struct{})
			go func() {
				for {
					select {
					case err := <-mq.Error():
						assertNoError(t, err, "Unexpected error from queue")
					case <-done:
						return
					}
				}
			}()

			expectedMessage := []byte("test")

			asyncProducer, err := mq.AsyncProducer(defaultAsyncProducerName)
			requireNoError(t, err, "Failed to get registered producer")
			asyncProducer.Produce(expectedMessage)

			syncProducer, err := mq.SyncProducer(defaultSyncProducerName)
			requireNoError(t, err, "Failed to get registered producer")
			err = syncProducer.Produce(expectedMessage)
			requireNoError(t, err, "Failed to produce message")

			var messageWasRead int32
			handler := func(message Message) {
				atomic.AddInt32(&messageWasRead, 1)

				if !isSliceOfBytesIsEqual(expectedMessage, message.Body()) {
					assertNoError(t, message.Reject(false))
					t.Errorf("Actual message '%s' is not equal to expected '%s'", message.Body(), expectedMessage)
				}

				assertNoError(t, message.Ack(false))
			}

			err = mq.SetConsumerHandler(defaultConsumerName, handler)
			requireNoError(t, err)

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
	requireNoError(t, broker.Start(), "Failed to start broker")

	messageQueue, err := New(Config{
		DSN:            dsnForTests,
		ReconnectDelay: time.Nanosecond,
		Consumers: Consumers{{
			Name:          defaultConsumerName,
			Queue:         defaultQueueName,
			PrefetchCount: 1,
			Workers:       1,
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

	requireNoError(t, err, "Can't create a new instance of mq")
	expectedMessage := []byte("test")

	var messageWasRead int32
	handler := func(message Message) {
		atomic.AddInt32(&messageWasRead, 1)

		if !isSliceOfBytesIsEqual(expectedMessage, message.Body()) {
			assertNoError(t, message.Reject(false))
			t.Errorf("Actual message '%s' is not equal to expected '%s'", message.Body(), expectedMessage)
		}

		assertNoError(t, message.Ack(false))
	}

	err = messageQueue.SetConsumerHandler(defaultConsumerName, handler)
	requireNoError(t, err, "Failed to set consumer handler")
	runtime.Gosched() // Required for running tests in build machine where only 1 cpu is available.

	// Force reconnect
	requireNoError(t, broker.Stop(), "Failed to stop broker")

	// Produce when connection to broker is down.
	asyncProducer, err := messageQueue.AsyncProducer(defaultAsyncProducerName)
	requireNoError(t, err, "Failed to get registered producer")
	asyncProducer.Produce(expectedMessage)

	syncProducer, err := messageQueue.SyncProducer(defaultSyncProducerName)
	requireNoError(t, err, "Failed to get registered producer")
	requireNoError(t, syncProducer.Produce(expectedMessage), "Sync producer error")

	requireNoError(t, broker.Start(), "Failed to start broker")
	defer broker.Stop()
	waitForMessageDelivery()

	readMessages := atomic.LoadInt32(&messageWasRead)
	if readMessages != 2 {
		t.Errorf("Consumer did not read messages. Produced %d, read %d", 2, readMessages)
	}

	messageQueue.Close()
}

func TestMq_Consumer_Exists(t *testing.T) {
	if brokerIsMocked {
		broker := server.NewServer(dsnForTests)
		requireNoError(t, broker.Start())
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

	requireNoError(t, err, "Can't create a new instance of mq")
	defer mq.Close()

	_, err = mq.Consumer(defaultConsumerName)
	assertNoError(t, err, "Failed to get a consumer")
	assertNoMqError(t, mq)
}

func TestMq_Consumer_NotExists(t *testing.T) {
	if brokerIsMocked {
		broker := server.NewServer(dsnForTests)
		requireNoError(t, broker.Start())
		defer broker.Stop()
	}

	mq, err := New(Config{
		DSN: dsnForTests,
	})

	requireNoError(t, err, "Can't create a new instance of mq")

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
		requireNoError(t, broker.Start())
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

	requireNoError(t, err, "Can't create a new instance of mq")
	defer mq.Close()

	_, err = mq.AsyncProducer(defaultAsyncProducerName)
	assertNoError(t, err, "Failed to get an async producer")

	_, err = mq.SyncProducer(defaultSyncProducerName)
	assertNoError(t, err, "Failed to get an sync producer")

	assertNoMqError(t, mq)
}

func TestMq_Producer_NonExistent(t *testing.T) {
	if brokerIsMocked {
		broker := server.NewServer(dsnForTests)
		requireNoError(t, broker.Start())
		defer broker.Stop()
	}

	mq, err := New(Config{
		DSN: dsnForTests,
	})

	requireNoError(t, err, "Can't create a new instance of mq")

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
		requireNoError(t, broker.Start())
		defer broker.Stop()
	}

	mq, err := New(Config{
		DSN: dsnForTests,
	})

	requireNoError(t, err, "Can't create a new instance of mq")

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

func Test_mq_createConnection(t *testing.T) {
	cases := []struct {
		name      string
		dsn       string
		checkList []string
	}{
		{name: "single dsn", dsn: dsnForTests, checkList: []string{dsnForTests}},
		{name: "cluster dsn", dsn: "amqp://guest:guest@localhost:5672/1,amqp://guest:guest@localhost:5672/2,amqp://guest:guest@localhost:5672/3",
			checkList: []string{"amqp://guest:guest@localhost:5672/1", "amqp://guest:guest@localhost:5672/2", "amqp://guest:guest@localhost:5672/3", "amqp://guest:guest@localhost:5672/1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newDefaultConfig()
			cfg.DSN = tc.dsn
			cfg.TestMode = true
			cfg.normalize()

			mq := &mq{
				config:               cfg,
				errorChannel:         make(chan error),
				internalErrorChannel: make(chan error),
				consumers:            newConsumersRegistry(len(cfg.Consumers)),
				producers:            newProducersRegistry(len(cfg.Producers)),
			}
			defer mq.Close()
			for _, dsn := range tc.checkList {
				_, _ = mq.createConnection()
				curBroker := mq.config.dsnList[mq.cluster.currentNode]
				if curBroker != dsn {
					t.Errorf("createConnection() current broker %v, expected broker %v", curBroker, dsn)
					return
				}
			}
		})
	}
}

func TestMq_ConnectionState(t *testing.T) {
	cases := []struct {
		name     string
		expected ConnectionState
	}{
		{name: "status undefined", expected: ConnectionStateUndefined},
		{name: "status changed", expected: ConnectionStateConnecting},
	}
	for _, tc := range cases {
		cfg := newDefaultConfig()
		cfg.TestMode = true
		cfg.normalize()

		mq := &mq{
			config:               cfg,
			errorChannel:         make(chan error),
			internalErrorChannel: make(chan error),
			consumers:            newConsumersRegistry(len(cfg.Consumers)),
			producers:            newProducersRegistry(len(cfg.Producers)),
			state:                new(int32),
		}
		atomic.StoreInt32(mq.state, int32(tc.expected))

		t.Run(tc.name, func(t *testing.T) {
			defer mq.Close()
			if mq.ConnectionState() != tc.expected {
				t.Errorf("ConnectionState() current value %v, expected broker %v", mq.ConnectionState(), tc.expected)
			}
		})
	}

}

func TestMq_connect(t *testing.T) {
	s := server.NewServer(dsnForTests)
	_ = s.Start()
	defer func() { _ = s.Stop() }()
	cases := []struct {
		name           string
		expected       ConnectionState
		isConnectError bool
		isChannelError bool
	}{
		{name: "success connect", expected: ConnectionStateConnected},
		{name: "failed to connect", expected: ConnectionStateDisconnected, isConnectError: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newDefaultConfig()
			cfg.TestMode = true
			cfg.normalize()

			mq := &mq{
				config:               cfg,
				errorChannel:         make(chan error),
				internalErrorChannel: make(chan error),
				consumers:            newConsumersRegistry(len(cfg.Consumers)),
				producers:            newProducersRegistry(len(cfg.Producers)),
				state:                new(int32),
			}
			defer mq.Close()
			if tc.isConnectError {
				_ = s.Stop()
			}
			err := mq.connect()
			if err != nil && !tc.isConnectError {
				t.Errorf("connect() no error expected, but got: %v", err)
			}
			if mq.ConnectionState() != tc.expected {
				t.Errorf("connect() expected state %v, got: %v", tc.expected, mq.ConnectionState())
			}
		})
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
	time.Sleep(10 * time.Millisecond)
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

func assertNoError(t *testing.T, err error, message ...string) {
	if err != nil {
		if len(message) == 0 {
			message = []string{"Unexpected error"}
		}

		t.Error(strings.Join(message, " ")+":", err)
	}
}

func requireNoError(t *testing.T, err error, message ...string) {
	if err != nil {
		if len(message) == 0 {
			message = []string{"Unexpected error"}
		}

		t.Fatal(strings.Join(message, " ")+":", err)
	}
}
