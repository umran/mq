package mq

// Message represents a standardized message format across all broker providers.
type Message struct {
	// Data represents the message payload as bytes.
	Data []byte

	// Attributes is an arbitrary key value map of string attributes.
	Attributes map[string]string
}
