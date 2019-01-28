package mq

import "github.com/NeowayLabs/wabbit"

// Describes producer for internal usages.
// Used to summarize common logic for sync and async producers.
type internalProducer interface {
	init()
	setChannel(channel wabbit.Channel)
	Stop()
}

func newInternalProducer(channel wabbit.Channel, errorChannel chan<- error, config ProducerConfig) internalProducer {
	if config.Sync {
		return newSyncProducer(channel, errorChannel, config)
	}

	return newAsyncProducer(channel, errorChannel, config)
}
