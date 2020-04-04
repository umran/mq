package mq

import (
	"errors"
	"testing"
)

func TestGCloudCreateTopic(t *testing.T) {
	conn, err := NewBroker(&Config{
		Provider:      "gcloud",
		GCloudProject: "cowrie-271900",
	})

	if err != nil {
		t.Error(err)
	}

	err = conn.CreateTopic("dep-processing")
	if err != nil {
		t.Error(err)
	}
}

func TestGCloudCreateSubscription(t *testing.T) {
	conn, err := NewBroker(&Config{
		Provider:      "gcloud",
		GCloudProject: "cowrie-271900",
	})

	if err != nil {
		t.Error(err)
	}

	err = conn.CreateSubscription("dep-processing-handler", &SubscriptionOptions{
		TopicID:           "dep-processing",
		AckDeadline:       10,
		RetentionDuration: 7 * 24 * 60 * 60,
	})

	if err != nil {
		t.Error(err)
	}
}

func TestGCloudPublish(t *testing.T) {
	conn, err := NewBroker(&Config{
		Provider:      "gcloud",
		GCloudProject: "cowrie-271900",
	})

	if err != nil {
		t.Error(err)
	}

	err = conn.Publish("dep-processing", &Message{
		Data: []byte("007"),
	})

	if err != nil {
		t.Error(err)
	}
}

func TestGCloudConsume(t *testing.T) {
	conn, err := NewBroker(&Config{
		Provider:      "gcloud",
		GCloudProject: "cowrie-271900",
	})

	if err != nil {
		t.Error(err)
	}

	err = conn.Publish("dep-processing", &Message{
		Data: []byte("007"),
	})

	if err != nil {
		t.Error(err)
	}

	msgs := make(chan string)
	errs := make(chan error)

	go func() {
		if err := conn.Consume("dep-processing-handler", func(msg *Message) error {
			msgs <- string(msg.Data)
			return nil
		}, &ConsumerOptions{
			MaxOutstandingMessages: 1,
			Concurrency:            1,
		}); err != nil {
			errs <- err
		}
	}()

	select {
	case msg := <-msgs:
		if msg != "007" {
			t.Error(errors.New("unexpected message"))
		}
	case err := <-errs:
		t.Error(err)
	}
}
