[![Build Status](https://travis-ci.org/cheshir/go-mq.svg?branch=master)](https://travis-ci.org/cheshir/go-mq)
[![codecov](https://codecov.io/gh/cheshir/go-mq/branch/master/graph/badge.svg)](https://codecov.io/gh/cheshir/go-mq)
[![Go Report Card](https://goreportcard.com/badge/cheshir/go-mq)](https://goreportcard.com/report/github.com/cheshir/go-mq)
[![Job Status](https://inspecode.rocro.com/badges/github.com/cheshir/go-mq/status?token=Te6_Jp4TGcnVzzX7WoPB5EpN5Pljhzll03ULZk0yi28)](https://inspecode.rocro.com/jobs/github.com/cheshir/go-mq/latest?completed=true)
[![GoDoc](https://godoc.org/github.com/cheshir/go-mq?status.svg)](https://godoc.org/github.com/cheshir/go-mq)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/cheshir/go-mq/blob/master/LICENSE)


# About

This package provides an ability to encapsulate creation and configuration of [AMQP](https://www.amqp.org) entities 
like queues, exchanges, producers and consumers in a declarative way with a single config.

Exchanges, queues and producers are going to be initialized in the background.

go-mq supports both sync and async producers.

go-mq has auto reconnects on closed connection or network error.
You can configure delay between each connect try using `reconnect_delay` option.

## Install

`go get -u github.com/cheshir/go-mq`

## API

Visit [godoc](https://godoc.org/github.com/cheshir/go-mq) to get information about library API.

For those of us who preferred learn something new on practice there is working examples in `example` directory.

## Configuration

Supported configuration tags:

* json
* yaml
* mapstructure

Available options:

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
    sync: false                          # Specify whether producer will worked in sync or async mode.
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

Errors from sync producer won't be accessible from error channel because they returned directly.

## Tests

There are some cases that can only be tested with real broker 
and some cases that can only be tested with mocked broker.
 
If you are able to run tests with a real broker run them with:

`go test -mock-broker=0`

Otherwise mock will be used.

## Changelog

Check [releases page](https://github.com/cheshir/go-mq/releases).

## How to upgrade

### From version 0.x to 1.x

* `GetConsumer()` method was renamed to `Consumer`. This is done to follow go guideline.

* `GetProducer()` method was removed. Use instead `AsyncProducer()` or `SyncProducer` if you want to catch net error by yourself.

## Epilogue

Feel free to create issues with bug reports or your wishes.
