package mq

// Message represents a standardized message format across all broker providers.
// The Data field represents the message payload as bytes.
// The Attributes field is an arbitrary key value map of strings.
type Message struct {
	Data       []byte
	Attributes map[string]string
}
