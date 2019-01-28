package main

import (
	"fmt"
	"log"
	"time"

	"github.com/cheshir/go-mq"
	"gopkg.in/yaml.v1"
)

var externalConfig = `
dsn: "amqp://guest:guest@localhost:5672/"
reconnect_delay: 1s
exchanges:
  - name: "demo"
    type: "direct"
    options:
      durable: true
queues:
  - name: "queue_name"
    exchange: "demo"
    routing_key: "key"
    options:
      durable: true
producers:
  - name: "async_producer"
    exchange: "demo"
    routing_key: "key"
    options:
      content_type: "text/plain"
      delivery_mode: 2
  - name: "sync_producer"
    exchange: "demo"
    routing_key: "key"
    sync: true
    options:
      content_type: "text/plain"
      delivery_mode: 2
consumers:
  - name: "consumer_name"
    queue: "queue_name"
    workers: 1
`

func main() {
	var config mq.Config
	err := yaml.Unmarshal([]byte(externalConfig), &config)
	if err != nil {
		log.Fatal("Failed to read config", err)
	}

	messageQueue, err := mq.New(config)
	if err != nil {
		log.Fatal("Failed to initialize message queue manager", err)
	}
	defer messageQueue.Close()

	go func() {
		for err := range messageQueue.Error() {
			log.Fatal("Caught error from message queue: ", err)
		}
	}()

	err = messageQueue.SetConsumerHandler("consumer_name", func(message mq.Message) {
		println(string(message.Body()))

		message.Ack(false)
	})
	if err != nil {
		log.Fatalf("Failed to set handler to consumer `%s`: %v", "consumer_name", err)
	}

	go func() {
		producer, err := messageQueue.SyncProducer("sync_producer")
		if err != nil {
			log.Fatal("Failed to get sync producer: ", err)
		}

		for i := 0; ; i++ {
			err = producer.Produce([]byte(fmt.Sprintf("Hello from sync producer #%d", i)))
			if err != nil {
				log.Fatal("Failed to send message from sync producer")
			}

			time.Sleep(time.Second)
		}
	}()

	producer, err := messageQueue.AsyncProducer("async_producer")
	if err != nil {
		log.Fatal("Failed to get async producer: ", err)
	}

	for i := 0; ; i++ {
		producer.Produce([]byte(fmt.Sprintf("Hello from async producer #%d", i)))
		time.Sleep(time.Second)
	}
}
