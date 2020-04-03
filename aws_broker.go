package mq

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type awsBroker struct {
	*sync.Mutex
	snsClient *sns.SNS
	sqsClient *sqs.SQS
	topicARNs map[string]string
	queueURLs map[string]string
}

// CreateTopic creates a new topic.
// This is an idempotent call and returns no error if the topic already exists.
func (conn *awsBroker) CreateTopic(topicID string) error {
	_, err := conn.snsClient.CreateTopic(&sns.CreateTopicInput{
		Name: aws.String(topicID),
	})

	return err
}

// CreateSubscription creates a new subscription to the topic specified in options.
// This is an idempotent call and returns no error if a subscription with the same id already exists,
// provided that the topic and other parameters are the same.
func (conn *awsBroker) CreateSubscription(subscriptionID string, options *SubscriptionOptions) error {
	// resolve topic arn first
	topicARN, err := conn.getTopicARN(options.TopicID)
	if err != nil {
		return err
	}

	// create the queue
	queue, err := conn.sqsClient.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(subscriptionID),
		Attributes: map[string]*string{
			"VisibilityTimeout":      aws.String(strconv.FormatInt(int64(options.AckDeadline), 10)),
			"MessageRetentionPeriod": aws.String(strconv.FormatInt(int64(options.RetentionDuration), 10)),
		},
	})

	if err != nil {
		return err
	}

	queueAttributes, err := conn.sqsClient.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl: queue.QueueUrl,
		AttributeNames: []*string{
			aws.String("QueueArn"),
			aws.String("Policy"),
		},
	})

	if err != nil {
		return err
	}

	queueARN := *queueAttributes.Attributes["QueueArn"]

	policy := newSqsPolicy(queueARN)
	if existingPolicy := queueAttributes.Attributes["Policy"]; existingPolicy != nil {
		policyBytes := json.RawMessage(*existingPolicy)

		json.Unmarshal(policyBytes, policy)
	}

	policy.AddPermission(queueARN, topicARN)
	policyBytes, _ := json.Marshal(policy)

	_, err = conn.sqsClient.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueUrl: queue.QueueUrl,
		Attributes: map[string]*string{
			"Policy": aws.String(string(policyBytes)),
		},
	})

	if err != nil {
		return err
	}

	// subscribe queue to topic
	_, err = conn.snsClient.Subscribe(&sns.SubscribeInput{
		TopicArn: &topicARN,
		Endpoint: &queueARN,
		Protocol: aws.String("sqs"),
	})

	return err
}

// Publish publishes a message to the specified topic.
func (conn *awsBroker) Publish(topicID string, message *Message) error {
	topicARN, err := conn.getTopicARN(topicID)
	if err != nil {
		return err
	}

	snsMessageAttributes := make(map[string]*sns.MessageAttributeValue)
	for attribute, value := range message.Attributes {
		snsMessageAttributes[attribute] = &sns.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(value),
		}
	}

	snsMessageInput := &sns.PublishInput{
		Message:  aws.String(string(message.Data)),
		TopicArn: &topicARN,
	}

	if len(snsMessageAttributes) > 0 {
		snsMessageInput.MessageAttributes = snsMessageAttributes
	}

	_, err = conn.snsClient.Publish(snsMessageInput)
	return err
}

// Consume consumes messages from the specified subscription
// and passes them on to the handler function.
// This is a blocking function and doesn't return until it encounters a network error.
func (conn *awsBroker) Consume(subsctiptionID string, handler func(*Message) error, options *ConsumerOptions) error {
	queueURL, err := conn.getQueueURL(subsctiptionID)
	if err != nil {
		return err
	}

	// create a buffered channel of messages
	msgs := make(chan *sqs.Message, options.MaxOutstandingMessages)
	// create a channel of errors
	errs := make(chan error)

	// launch a routine to consume messages
	go func() {
		for {
			response, err := conn.sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueUrl:            &queueURL,
				MaxNumberOfMessages: aws.Int64(int64(options.MaxOutstandingMessages)),
				WaitTimeSeconds:     aws.Int64(2),
			})

			if err != nil {
				errs <- err
				return
			}

			if len(response.Messages) == 0 {
				continue
			}

			for _, msg := range response.Messages {
				msgs <- msg
			}
		}
	}()

	for {
		select {
		case msg := <-msgs:
			if err := conn.handleMessage(msg, queueURL, handler); err != nil {
				return err
			}
		case err := <-errs:
			return err
		}
	}
}

func (conn *awsBroker) handleMessage(outerMsg *sqs.Message, queueURL string, handler func(*Message) error) error {
	outerData := json.RawMessage(*outerMsg.Body)

	msg := &snsMessage{}
	if err := json.Unmarshal(outerData, &msg); err != nil {
		return err
	}

	attributes := make(map[string]string)
	for attribute, value := range msg.MessageAttributes {
		if value.Type == "String" {
			attributes[attribute] = value.Value
		}
	}

	err := handler(&Message{
		Data:       []byte(msg.Message),
		Attributes: attributes,
	})

	if err == nil {
		// acknowledge processing by deleting the message from queue
		// we can safely ignore delete errors because message processing is assumed to be idempotent
		conn.sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      &queueURL,
			ReceiptHandle: outerMsg.ReceiptHandle,
		})
	}

	return nil
}

func (conn *awsBroker) getQueueURL(subscriptionID string) (string, error) {
	queueURL := conn.queueURLs[subscriptionID]
	if queueURL == "" {
		conn.Lock()
		defer conn.Unlock()

		queueURLResult, err := conn.sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{
			QueueName: aws.String(subscriptionID),
		})

		if err != nil {
			return queueURL, err
		}

		queueURL = *queueURLResult.QueueUrl
		conn.queueURLs[subscriptionID] = queueURL
	}

	return queueURL, nil
}

func (conn *awsBroker) getTopicARN(topicID string) (string, error) {
	topicARN := conn.topicARNs[topicID]

	if topicARN == "" {
		conn.Lock()
		defer conn.Unlock()
		nextToken := ""
	search:
		for {
			listTopicsInput := &sns.ListTopicsInput{}
			if nextToken != "" {
				listTopicsInput.NextToken = &nextToken
			}

			response, err := conn.snsClient.ListTopics(listTopicsInput)
			if err != nil {
				return topicARN, err
			}

			for _, topic := range response.Topics {
				if strings.HasSuffix(*topic.TopicArn, fmt.Sprintf(":%s", topicID)) {
					topicARN = *topic.TopicArn
					conn.topicARNs[topicID] = topicARN
					break search
				}
			}

			if response.NextToken != nil {
				nextToken = *response.NextToken
			} else {
				return topicARN, errors.New("topic not found")
			}
		}
	}

	return topicARN, nil
}

func newAwsBroker(region string) (Broker, error) {
	session, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})

	if err != nil {
		return nil, err
	}

	conn := &awsBroker{
		Mutex:     new(sync.Mutex),
		snsClient: sns.New(session),
		sqsClient: sqs.New(session),
		topicARNs: make(map[string]string),
		queueURLs: make(map[string]string),
	}

	return conn, nil
}
