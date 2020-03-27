package mq

import "errors"

// Broker ...
type Broker interface {
	// CreateTopic creates a new topic with the name `topicID`.
	// This is an idempotent call and returns no error if the topic already exists.
	CreateTopic(topicID string) error

	// CreateSubscription creates a new subscription with the name `subscriptionID` that
	// is subscribed to the topic specified in options.
	// This is an idempotent call and returns no error if a subscription with the same id already exists,
	// provided that the topic and other parameters are the same.
	CreateSubscription(subscriptionID string, options *SubscriptionOptions) error

	// Publish publishes a message to the given topic.
	Publish(topicID string, message *Message) error

	// Subscribe consumes messages from the specified subscription
	// and passes them on to the handler function.
	// This is a blocking function and doesn't return until it encounters an error.
	Subscribe(subscriptionID string, handler func(*Message) error) error
}

// NewBroker ...
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
