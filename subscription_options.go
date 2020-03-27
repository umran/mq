package mq

// SubscriptionOptions ...
type SubscriptionOptions struct {
	TopicID           string
	AckDeadline       int
	RetentionDuration int
}
