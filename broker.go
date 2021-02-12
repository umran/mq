package mq

import "errors"

// Broker is an interface that represents an underlying cloud message broker.
type Broker interface {
	// CreateTopic creates a new topic.
	// This is an idempotent call and returns no error if the topic already exists.
	CreateTopic(topicID string) error

	// CreateSubscription creates a new subscription to the topic specified in options.
	// This is an idempotent call and returns no error if a subscription with the same id already exists,
	// provided that the topic and other parameters are the same.
	CreateSubscription(subscriptionID string, options *SubscriptionOptions) error

	// Publish publishes a message to the specified topic.
	Publish(topicID string, message *Message) error

	// Consume consumes messages from the specified subscription
	// and passes them on to the handler function.
	// This is a blocking function and doesn't return until it encounters a network error.
	Consume(subscriptionID string, handler func(*Message) error, options *ConsumerOptions) error
}

// NewBroker returns a new broker configured for an underlying cloud provider.
func NewBroker(config *Config) (Broker, error) {
	switch config.Provider {
	case ProviderAWS:
		return newAwsBroker(config.AWSRegion)
	case ProviderGCP:
		return newGcloudBroker(config.GCloudProject)
	default:
		return nil, errors.New("unrecognized provider")
	}
}
