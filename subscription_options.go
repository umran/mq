package mq

// SubscriptionOptions represents the configuration of a subscription
// TopicID represents the topic to which the subscription is made
// AckDeadline is the duration (in seconds) within which a consumer must
// acknowledge processing of a message before it is resent to the queue.
// RetentionDuration is the duration (in seconds) for which messages are
// kept in the queue before they are deleted.
type SubscriptionOptions struct {
	TopicID           string
	AckDeadline       int
	RetentionDuration int
}
