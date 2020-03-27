package mq

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/pubsub"
)

// GCloudConnection ...
type GCloudConnection struct {
	context context.Context
	client  *pubsub.Client
}

// CreateTopic ...
func (conn *GCloudConnection) CreateTopic(topicID string) error {
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

// CreateSubscription ...
func (conn *GCloudConnection) CreateSubscription(subscriptionID string, options *SubscriptionOptions) error {
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
		// perform a further check to see if it is subscribed to the intended topic
		config, err := subscription.Config(conn.context)
		if err != nil {
			return err
		}

		if config.Topic.ID() != options.TopicID {
			return errors.New("a subscription by that name already exists and is subscribed to another topic")
		}

		return nil
	}

	_, err = conn.client.CreateSubscription(conn.context, subscriptionID, pubsub.SubscriptionConfig{
		Topic:             topic,
		AckDeadline:       time.Duration(options.AckDeadline) * time.Second,
		RetentionDuration: time.Duration(options.RetentionDuration) * time.Second,
		ExpirationPolicy:  time.Duration(0),
	})

	return err
}

// Publish ...
func (conn *GCloudConnection) Publish(topicID string, message *Message) error {
	topic := conn.client.Topic(topicID)
	defer topic.Stop()

	r := topic.Publish(conn.context, &pubsub.Message{
		Data:       message.Data,
		Attributes: message.Attributes,
	})

	_, err := r.Get(conn.context)
	return err
}

// Subscribe ...
func (conn *GCloudConnection) Subscribe(subscriptionID string, handler func(*Message) error) error {
	subscription := conn.client.Subscription(subscriptionID)

	return subscription.Receive(conn.context, func(ctx context.Context, msg *pubsub.Message) {
		if err := handler(&Message{
			Data:       msg.Data,
			Attributes: msg.Attributes,
		}); err != nil {
			// we can safely ignore nack errors because message processing is assumed to be idempotent
			msg.Nack()
		}

		// we can safely ignore ack errors because message processing is assumed to be idempotent
		msg.Ack()
	})
}

// NewGCloudConnection ...
func NewGCloudConnection(project string) (*GCloudConnection, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}

	conn := &GCloudConnection{
		context: ctx,
		client:  client,
	}

	return conn, nil
}
