# About

This package provides an ability to encapsulate creation and configuration of [AMQP](https://www.amqp.org) entities 
like queues, exchanges, producers and consumers in a declarative way with a single config.

Status: **WIP**.

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
        true,    // durable
        false,   // delete when unused
        false,   // exclusive
        false,   // no-wait
        nil,     // arguments
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
        "demo", // exchange
        "route", // routing key
        false,  // mandatory
        false,  // immediate
        amqp.Publishing {
          ContentType: "text/plain",
          Body:        []byte(body),
        },
    )
    if err != nil {
        panic(err)
    }
}
```

Looks too verbose if you ask me.

### Same hello world with go-mq

Config file:

```yaml
mq:
  dsn: "amqp://guest:guest@localhost:5672/"
  exchanges:                # Exchange list.
    demo:                   # Exchange name.
      type: "direct"
      options:
      	durable: true
  queues:                   # Queue list.
    hello_q:                # Queue name.
      exchange: "demo"      # Link to exchange "demo".
      routing_key: "route"
      options:
        durable: true
  producers:                # Producer list.
    hello_p:                # Producer name.
      exchange: "demo"      # Link to exchange "demo".
      routing_key: "route"
      options:
        content_type: "text/plain"

```

And then:

```go
package main

import (
	"log"

    "github.com/cheshir/go-mq"
    "github.com/spf13/viper"
)

func main() {
    // Reads config.
    // TODO in task #2.

	queue, err := mq.New(viper.Sub("mq"))
	if err != nil {
		panic("Error during initializing RabbitMQ: " + err.Error())
	}
	defer queue.Close()
	
	// Get producer by its name.
	producer, ok := queue.GetProducer("hello_p")
	if !ok {
		panic("Trying to get unknown producer")
	}
	
	body := "hello world!"
	producer.Produce([]byte(body))
}
```

Exchanges, queues and producers are going to be initialized in the background.

You can get concrete producer with `queue.GetProducer()`.

## Config in depth

```yaml
dsn: "amqp://login:password@host:port/virtual_host"
reconnect_timeout: 5                     # Interval between connection tries in seconds.
exchanges:                               # Exchange list.
  exchange_name:
    type: "direct"
    options:
      # Available options with default values:
      durable: false
      auto_delete: false
      autoDelete: false
      internal: false
      no_wait: false
      noWait: false
queues:                                  # Queue list.
  queue_name:
    exchange: "exchange_name"
    routing_key: "route"
    # A set of arguments for the binding.
    # The syntax and semantics of these arguments depend on the exchange class.
    binding_options:
      no_wait: false
      noWait: false
    options:
      durable: false
      auto_delete: false
      autoDelete: false
      exclusive: false
      no_wait: false
      noWait: false
producers:                               # Producer list.
  producer_name:
    exchange: "exchange_name"
    routing_key: "route"
    buffer_size: 100                     # Declare how many messages we can buffer during fat messages publishing.
    # Available options:
    options:
      deliveryMode: 2                    # 1 - non persistent, 2 - persistent.
      contentType:  "application/json"
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
	queue, _ := mq.New(viper.Sub("config_key"))
	// ...

	go handleErrors(queue.Error())
	
	// Other logic.
}

func handleErrors(errors <-chan error) {
	for err := range errors {
		log.Println(err)
	}
}
```

## Epilogue

Feel free to create issues with bug reports or your wishes.

## License

go-mq is released under the Apache 2.0 license.
