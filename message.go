package mq

// Message represents a standardized message format across all broker providers.
type Message struct {
	// Data represents the message payload as bytes.
	Data []byte

	// Attributes is an arbitrary key value map of string attributes.
	Attributes map[string]string

	// OrderingKey is a key that enables messages assigned with the key to be ordered
	// if consuming from a Subscription for which EnableMessageOrdering = true
	// Defaults to the empty string: "".
	// CAUTION: Sub ordering by message key only applies to Google Pub Sub Subscriptions.
	// If using the AWS Provider and EnableMessageOrdering = true, all messages are ordered
	// regardless of OrderingKey.
	OrderingKey string
}
