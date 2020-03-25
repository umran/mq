package mq

// Message ...
type Message struct {
	Data       []byte
	Attributes map[string]string
}
