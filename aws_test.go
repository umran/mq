package mq

import (
	"errors"
	"testing"
)

func TestAWSPublisher(t *testing.T) {
	conn, err := NewAWSConnection("us-west-2")
	if err != nil {
		t.Error(err)
	}

	err = conn.Publish("deposit-processing", &Message{
		Data: []byte("007"),
		Attributes: map[string]string{
			"service": "deposit",
		},
	})

	if err != nil {
		t.Error(err)
	}
}

func TestAWSSubscriber(t *testing.T) {
	conn, err := NewAWSConnection("us-west-2")
	if err != nil {
		t.Error(err)
	}

	err = conn.Publish("deposit-processing", &Message{
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
		if err := conn.Subscribe("deposit-processing", func(msg *Message) error {
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
