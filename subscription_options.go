package mq

// SubscriptionOptions represents configuration options for a subscription.
type SubscriptionOptions struct {
	// TopicID represents the subscription topic.
	TopicID string

	// EnableMessageOrdering is a boolean for whether messages with the same
	// ordering key should be ordered according to when they were published.
	// Creates a FIFO queue if using AWS SQS
	// Defaults to false
	EnableMessageOrdering bool

	// AckDeadline is the duration (in seconds) within which a consumer must
	// acknowledge processing of a message before it is resent to the queue.
	// Defaults to 10
	AckDeadline int

	// RetentionDuration is the duration (in seconds) for which messages are
	// kept in the queue before they are deleted. Defaults to 604800
	RetentionDuration int

	// ExpirationPolicy is the duration (in seconds) after which the subscription
	// should automatically be deleted if there is no active subscriber
	// activity. This value should be longer than RetentionDuration.
	// Defaults to 0, meaning it never expires
	// CAUTION: This option only applies to Google Pub Sub Subscriptions
	ExpirationPolicy int
}
