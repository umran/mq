package mq

import (
	"context"

	"cloud.google.com/go/pubsub"
)

// GCloudConnection ...
type GCloudConnection struct {
	context context.Context
	client  *pubsub.Client
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
