package mq

// Broker ...
type Broker interface {
	CreateTopic(topicID string) error
	CreateSubscription(subscriptionID string, options *SubscriptionOptions) error
	Publish(topicID string, message *Message) error
	Subscribe(subscriptionID string, handler func(*Message) error) error
}
