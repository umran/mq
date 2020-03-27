package mq

// Publisher ...
type Publisher interface {
	Publish(topicID string, message *Message) error
}

// Subscriber ...
type Subscriber interface {
	Subscribe(subscriptionID string, handler func(*Message) error) error
}

// PublisherSubscriber ...
type PublisherSubscriber interface {
	Publish(topicID string, message *Message) error
	Subscribe(subscriptionID string, handler func(*Message) error) error
}

// Administrator ...
type Administrator interface {
	CreateTopic(topicID string) error
	CreateSubscription(subscriptionID string, options *SubscriptionOptions) error
}
