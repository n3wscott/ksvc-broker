package main

import (
	"github.com/n3wscott/ksvc-broker/pkg/broker"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/signals"
)

func main() {
	ctx := signals.NewContext()
	logger := logging.FromContext(ctx)

	b, err := broker.NewBroker(logger)
	if err != nil {
		logger.Error(err)
	}
	if err := b.Start(ctx); err != nil {
		logger.Error(err)
	}
}
