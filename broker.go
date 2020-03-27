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

	// Subscribe consumes messages from the specified subscription
	// and passes them on to the handler function.
	// This is a blocking function and doesn't return until it encounters a network error.
	Subscribe(subscriptionID string, handler func(*Message) error) error
}

// NewBroker returns a new broker configured for an underlying cloud provider.
func NewBroker(config *Config) (Broker, error) {
	switch config.Provider {
	case "aws":
		return newAwsBroker(config.AWSRegion)
	case "gcloud":
		return newGcloudBroker(config.GCloudProject)
	default:
		return nil, errors.New("unrecognized provider")
	}
}
