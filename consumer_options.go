package mq

// ConsumerOptions represents options for the way messages are to
// be consumed and handled from the queue
type ConsumerOptions struct {
	// The maximum number of messages to lease from the queue at any given time.
	MaxOutstandingMessages int

	// The maximum number of go routines to spawn for message processing
	Concurrency int
}
