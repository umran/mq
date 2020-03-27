package mq

type snsMessage struct {
	Message           string
	MessageAttributes map[string]*snsMessageAttribute
}

type snsMessageAttribute struct {
	Type  string
	Value string
}
