package mq

// SubscriptionOptions represents configuration options for a subscription.
type SubscriptionOptions struct {
	// TopicID represents the subscription topic.
	TopicID string

	// AckDeadline is the duration (in seconds) within which a consumer must
	// acknowledge processing of a message before it is resent to the queue.
	AckDeadline int

	// RetentionDuration is the duration (in seconds) for which messages are
	// kept in the queue before they are deleted.
	RetentionDuration int
}
