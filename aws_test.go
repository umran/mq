package mq

import (
	"errors"
	"testing"
)

func TestAWSCreateTopic(t *testing.T) {
	conn, err := NewBroker(&Config{
		Provider:  "aws",
		AWSRegion: "us-west-2",
	})

	if err != nil {
		t.Error(err)
	}

	err = conn.CreateTopic("umt")
	if err != nil {
		t.Error(err)
	}
}

func TestAWSCreateSubscription(t *testing.T) {
	conn, err := NewBroker(&Config{
		Provider:  "aws",
		AWSRegion: "us-west-2",
	})

	if err != nil {
		t.Error(err)
	}

	err = conn.CreateSubscription("umt-handler", &SubscriptionOptions{
		TopicID:           "umt",
		AckDeadline:       10,
		RetentionDuration: 7 * 24 * 60 * 60,
	})

	if err != nil {
		t.Error(err)
	}
}

func TestAWSPublish(t *testing.T) {
	conn, err := NewBroker(&Config{
		Provider:  "aws",
		AWSRegion: "us-west-2",
	})

	if err != nil {
		t.Error(err)
	}

	err = conn.Publish("umt", &Message{
		Data: []byte("007"),
		Attributes: map[string]string{
			"service": "deposit",
		},
	})

	if err != nil {
		t.Error(err)
	}
}

func TestAWSSubscribe(t *testing.T) {
	conn, err := NewBroker(&Config{
		Provider:  "aws",
		AWSRegion: "us-west-2",
	})

	if err != nil {
		t.Error(err)
	}

	err = conn.Publish("umt", &Message{
		Data: []byte("007"),
		Attributes: map[string]string{
			"service": "deposit",
		},
	})

	if err != nil {
		t.Error(err)
	}

	msgs := make(chan string)
	errs := make(chan error)

	go func() {
		if err := conn.Subscribe("umt-handler", func(msg *Message) error {
			msgs <- string(msg.Data)
			return nil
		}); err != nil {
			errs <- err
		}
	}()

	select {
	case msg := <-msgs:
		if msg != "007" {
			t.Error(errors.New("unexpected message"))
			t.Log(msg)
		}
	case err := <-errs:
		t.Error(err)
	}
}
