package mq

import (
	"context"
	"errors"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
)

type gcloudBroker struct {
	mutex         *sync.RWMutex
	context       context.Context
	client        *pubsub.Client
	publishTopics map[string]*pubsub.Topic
}

// CreateTopic creates a new topic.
// This is an idempotent call and returns no error if the topic already exists.
func (conn *gcloudBroker) CreateTopic(topicID string) error {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()

	topic := conn.client.Topic(topicID)

	// check if the topic exists
	exists, err := topic.Exists(conn.context)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	_, err = conn.client.CreateTopic(conn.context, topicID)
	return err
}

// CreateSubscription creates a new subscription to the topic specified in options.
// This is an idempotent call and returns no error if a subscription with the same id already exists,
// provided that the topic and other parameters are the same.
func (conn *gcloudBroker) CreateSubscription(subscriptionID string, options *SubscriptionOptions) error {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()

	// set option defaults if not set
	if options.AckDeadline <= 0 {
		options.AckDeadline = 10
	}

	if options.RetentionDuration <= 0 {
		options.RetentionDuration = 604800
	}

	if options.ExpirationPolicy < 0 {
		options.ExpirationPolicy = 0
	}

	topic := conn.client.Topic(options.TopicID)
	subscription := conn.client.Subscription(subscriptionID)

	existsTopic, err := topic.Exists(conn.context)
	if err != nil {
		return err
	}

	if !existsTopic {
		return errors.New("topic does not exist")
	}

	// check if the subscription exists
	existsSubscription, err := subscription.Exists(conn.context)
	if err != nil {
		return err
	}

	if existsSubscription {
		// perform a further check to see if it has an identical configuration
		config, err := subscription.Config(conn.context)
		if err != nil {
			return err
		}

		if config.Topic.ID() != options.TopicID {
			return errors.New("a subscription by that name already exists and is subscribed to a different topic")
		}

		if config.EnableMessageOrdering != options.EnableMessageOrdering {
			return errors.New("a subscription by that name already exists with a different value for EnableMessageOrdering")
		}

		if config.AckDeadline != time.Duration(options.AckDeadline)*time.Second {
			return errors.New("a subscription by that name already exists with a different AckDeadline")
		}

		if config.RetentionDuration != time.Duration(options.RetentionDuration)*time.Second {
			return errors.New("a subscription by that name already exists with a different RetentionDuration")
		}

		if (config.ExpirationPolicy != nil && config.ExpirationPolicy != time.Duration(options.ExpirationPolicy)*time.Second) || (config.ExpirationPolicy == nil && options.ExpirationPolicy != 0) {
			return errors.New("a subscription by that name already exists with a different Expiration Policy")
		}

		return nil
	}

	_, err = conn.client.CreateSubscription(conn.context, subscriptionID, pubsub.SubscriptionConfig{
		Topic:                 topic,
		AckDeadline:           time.Duration(options.AckDeadline) * time.Second,
		RetentionDuration:     time.Duration(options.RetentionDuration) * time.Second,
		ExpirationPolicy:      time.Duration(options.ExpirationPolicy) * time.Second,
		EnableMessageOrdering: options.EnableMessageOrdering,
	})

	return err
}

// Publish publishes a message to the specified topic.
func (conn *gcloudBroker) Publish(topicID string, message *Message) error {
	topic := conn.getPublishTopic(topicID)

	if message.OrderingKey != "" {
		topic.EnableMessageOrdering = true
	}

	r := topic.Publish(conn.context, &pubsub.Message{
		Data:        message.Data,
		Attributes:  message.Attributes,
		OrderingKey: message.OrderingKey,
	})

	_, err := r.Get(conn.context)
	return err
}

// Consume consumes messages from the specified subscription
// and passes them on to the handler function.
// This is a blocking function and doesn't return until it encounters a network error.
func (conn *gcloudBroker) Consume(subscriptionID string, handler func(*Message) error, options *ConsumerOptions) error {
	subscription := conn.client.Subscription(subscriptionID)
	subscription.ReceiveSettings.NumGoroutines = options.Concurrency
	subscription.ReceiveSettings.MaxOutstandingMessages = options.MaxOutstandingMessages

	return subscription.Receive(conn.context, func(ctx context.Context, msg *pubsub.Message) {
		if err := handler(&Message{
			Data:       msg.Data,
			Attributes: msg.Attributes,
		}); err != nil {
			// we can safely ignore nack errors because message processing is assumed to be idempotent
			msg.Nack()
			return
		}

		// we can safely ignore ack errors because message processing is assumed to be idempotent
		msg.Ack()
	})
}

func (conn *gcloudBroker) getPublishTopic(topicID string) *pubsub.Topic {
	conn.mutex.RLock()
	topic, ok := conn.publishTopics[topicID]
	if ok {
		defer conn.mutex.RUnlock()
		return topic
	}

	conn.mutex.RUnlock()
	conn.mutex.Lock()
	defer conn.mutex.Unlock()

	topic, ok = conn.publishTopics[topicID]
	if ok {
		return topic
	}

	topic = conn.client.Topic(topicID)
	conn.publishTopics[topicID] = topic
	return topic
}

func newGcloudBroker(project string) (Broker, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}

	conn := &gcloudBroker{
		mutex:         new(sync.RWMutex),
		context:       ctx,
		client:        client,
		publishTopics: make(map[string]*pubsub.Topic),
	}

	return conn, nil
}
