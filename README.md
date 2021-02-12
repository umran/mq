# MQ - A Cloud-Agnostic Go Library for Publishing to and Consuming from Message Queues
This library exposes a simple interface with methods to publish to and consume from an underlying message queue. Implementations for AWS SNS+SQS and Google Cloud PubSub come out of the box, with future plans to implement other providers like RabbitMQ and Kafka.

## Installation
```
go get github.com/umran/mq
```

## Creating a Broker Configured for Google Cloud PubSub
```go
broker, err := mq.Broker(&mq.Config{
    Provider: mq.ProviderGCP,
})
```

## Creating a Broker Configured for AWS SNS+SQS
```go
broker, err := mq.Broker(&mq.Config{
    Provider: mq.ProviderAWS,
})
```

## Usage
Once a Broker is created, the rest of the API behaves the same regardless of the underlying provider

### Creating a Topic
```go
err := broker.CreateTopic("Topic_ID")
```

### Creating a Subscription to a Topic
This operation is equivalent to creating a queue and subscribing the queue to a specified topic
```go
err := broker.CreateSubscription("Subscription_ID", &mq.SubscriptionOptions{
    TopicID: "Topic_ID" // the ID of the topic to subscribe to,
    AckDeadline:       10,
	RetentionDuration: 7 * 24 * 60 * 60,
})
```

### Publishing to a Topic
```go
err := broker.Publish("Topic_ID", &mq.Message{
    Data: []byte("this is a message"),
    // Attributes are an optional set of key value pairs of strings
    Attributes: map[string]string{
        "key": "value",
    },
})
```

### Consuming from a Subscription
Consume listens for new messages published to a subscription and processes them according to a handler function.
The return type of the handler function is an error. If the handler function returns an error, the message is nacked, thereby causing the message to be republished. If the handler function returns no error, the message is acked. However, since arbitrary failures such as network failure can prevent messages that have already been successfully processed from being acked, there is no guarantee of exactly once delivery. For this reason, the handler function should be idempotent.

This method also requires a second argument, which specifies some parameters that determine the behaviour of the consuming process.

Note also that this is a blocking operation as it keeps an open connection while listening for new messages
```go
err := broker.Consume("Subscription_ID", func(msg *mq.Message) error {
    fmt.Println(string(msg.Data))
    return nil
}, &mq.ConsumerOptions{
    MaxOutstandingMessages: 1,  // the number of messages to take off the queue at a time  
    Concurrency:            1,   // the number of go routines to deploy for handling messages
})
```
