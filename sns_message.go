package mq

type snsMessage struct {
	Message           string
	MessageAttributes map[string]*snsAttribute
}

type snsAttribute struct {
	Type  string
	Value string
}
