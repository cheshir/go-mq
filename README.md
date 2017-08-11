[![Build Status](https://travis-ci.org/cheshir/go-mq.svg?branch=master)](https://travis-ci.org/cheshir/go-mq)
[![codecov](https://codecov.io/gh/cheshir/go-mq/branch/master/graph/badge.svg)](https://codecov.io/gh/cheshir/go-mq)
[![Go Report Card](https://goreportcard.com/badge/cheshir/go-mq)](https://goreportcard.com/report/github.com/cheshir/go-mq)
[![GoDoc](https://godoc.org/github.com/cheshir/go-mq?status.svg)](https://godoc.org/github.com/cheshir/go-mq)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/cheshir/go-mq/blob/master/LICENSE)


# About

This package provides an ability to encapsulate creation and configuration of [AMQP](https://www.amqp.org) entities 
like queues, exchanges, producers and consumers in a declarative way with a single config.

## Install

`go get github.com/cheshir/go-mq`

## Example

### Hello world with default AMQP library

```go
package main

import (
	"github.com/streadway/amqp"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()
	
	q, err := ch.QueueDeclare(
		"hello_q", // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		panic(err)
	}
	
	err = ch.ExchangeDeclare(
	  "demo",   // name
	  "direct", // type
	  true,     // durable
	  false,    // auto-deleted
	  false,    // internal
	  false,    // no-wait
	  nil,      // arguments
	)
	
	err = ch.QueueBind(q.Name, "route", "demo", false, nil)
	if err != nil {
		panic(err)
	}
	
	body := "hello world!"
	err = ch.Publish(
		"demo",  // exchange
		"route", // routing key
		false,   // mandatory
		false,   // immediate
		amqp.Publishing {
		  ContentType: "text/plain",
		  Body:        []byte(body),
		},
	)
	if err != nil {
		panic(err)
	}
	
	deliveries, err := ch.Consume(
		q.Name, // name
		"",       // consumer tag
		false,    // auto ack
		false,    // exclusive
		false,    // no-local
		false,    // no-wait,
		nil,
	)
	if err != nil {
		panic(err)
	}
	
	handleMessages(deliveries)
}

func handleMessages(deliveries <-chan amqp.Delivery) {
	for message := range deliveries {
		// Process message here.
		message.Ack(false)
	}
}
```

Looks too verbose if you ask me.

### Same hello world with go-mq

Config file:

```yaml
mq:
  dsn: "amqp://guest:guest@localhost:5672/"
  exchanges:
    - name: "demo"
      type: "direct"
      options:
        durable: true
  queues:
    - name: "hello_q"
      exchange: "demo"      # Link to exchange "demo".
      routing_key: "route"
      options:
        durable: true
  producers:
    - name: "hello_p"
      exchange: "demo"      # Link to exchange "demo".
      routing_key: "route"
      options:
        content_type: "text/plain"
  consumers:
    - name: "hello_c"
      queue: "hello_q"      # Link to queue
      workers: 1
```

And then we're going to read config with [Viper](https://github.com/spf13/viper) and produce message to the queue:

```go
package main

import (
	"github.com/cheshir/go-mq"
	"github.com/spf13/viper"
)

func init() {
    // Configure viper.
}

func main() {
	var config mq.Config
	if err := viper.Sub("mq").Unmarshal(&config); err != nil {
		panic(err)
	}

	queue, err := mq.New(config)
	if err != nil {
		panic("Error during initializing RabbitMQ: " + err.Error())
	}
	defer queue.Close()
	
	// Get producer by its name.
	producer, err := queue.GetProducer("hello_p")
	if err != nil {
		panic(err)
	}
	
	body := "hello world!"
	producer.Produce([]byte(body))
	
	consumer, err := queue.GetConsumer("hello_c")
	if err != nil {
		panic("Trying to get unknown consumer")
	}
	consumer.Consume(handleMessages)
	
	// Or you can use a little bit shorter approach:
	// if err := queue.SetConsumerHandler("hello_c", handleMessages); err != nil {
	//	  panic(err)
	// }
	
	select {}
}

func handleMessages(message mq.Message) {
	// Process message here.
	message.Ack(false)
}
```

Of course, you can fill `mq.Config` without `Viper`. Supported tags:

* json
* yaml
* mapstructure

Exchanges, queues and producers are going to be initialized in the background.

You can get concrete producer with `queue.GetProducer()`.

## Config in depth

```yaml
dsn: "amqp://login:password@host:port/virtual_host"
reconnect_delay: 5s                     # Interval between connection tries. Check https://golang.org/pkg/time/#ParseDuration for details.
exchanges:
  - name: "exchange_name"
    type: "direct"
    options:
      # Available options with default values:
      auto_delete: false
      durable: false
      internal: false
      no_wait: false
queues:
  - name: "queue_name"
    exchange: "exchange_name"
    routing_key: "route"
    # A set of arguments for the binding.
    # The syntax and semantics of these arguments depend on the exchange class.
    binding_options:
      no_wait: false
    # Available options with default values:
    options:
      auto_delete: false
      durable: false
      exclusive: false
      no_wait: false
producers:
  - name: "producer_name"
    buffer_size: 10                      # Declare how many messages we can buffer during fat messages publishing.
    exchange: "exchange_name"
    routing_key: "route"
    # Available options with default values:
    options:
      content_type:  "application/json"
      delivery_mode: 2                   # 1 - non persistent, 2 - persistent.
consumers:
  - name: "consumer_name"
    queue: "queue_name"
    workers: 1                           # Workers count. Defaults to 1.
    prefetch_count: 0                    # Prefetch message count per worker.
    prefetch_size: 0                     # Prefetch message size per worker.
    # Available options with default values:
    options:
      no_ack: false
      no_local: false
      no_wait: false
      exclusive: false
```

## Error handling

All errors are accessible via exported channel:

```go
package main

import (
	"log"

	"github.com/cheshir/go-mq"
	"github.com/spf13/viper"
)

func main() {
	config := mq.Config{} // Set your configuration.
	queue, _ := mq.New(config)
	// ...

	go handleMQErrors(queue.Error())
	
	// Other logic.
}

func handleMQErrors(errors <-chan error) {
	for err := range errors {
		log.Println(err)
	}
}
```

If channel is full – new errors will be dropped.

# Reconnect

go-mq will try to reconnect on closed connection or network error. 

You can set delay between each try with `reconnect_delay` option.

# Tests

There are some cases that can only be tested with real broker 
and some cases that can only be tested with mocked broker.
 
If you are able to run tests with a real broker run them with:

`go test -mock-broker=0`

Otherwise mock will be used.

## Epilogue

Feel free to create issues with bug reports or your wishes.
