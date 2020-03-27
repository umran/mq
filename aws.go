package mq

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type awsBroker struct {
	snsClient *sns.SNS
	sqsClient *sqs.SQS
	topicARNs map[string]string
	queueURLs map[string]string
}

// CreateTopic ...
func (conn *awsBroker) CreateTopic(topicID string) error {
	_, err := conn.snsClient.CreateTopic(&sns.CreateTopicInput{
		Name: aws.String(topicID),
	})

	return err
}

// CreateSubscription ...
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
	if rawPolicy := queueAttributes.Attributes["Policy"]; rawPolicy != nil {
		policyBytes := json.RawMessage(*rawPolicy)

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

// Publish ...
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

// Subscribe ...
func (conn *awsBroker) Subscribe(subsctiptionID string, handler func(*Message) error) error {
	queueURL, err := conn.getQueueURL(subsctiptionID)
	if err != nil {
		return err
	}

	// start blocking
	for {
		response, err := conn.sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            &queueURL,
			MaxNumberOfMessages: aws.Int64(1),
			WaitTimeSeconds:     aws.Int64(20),
		})

		if err != nil {
			return err
		}

		if len(response.Messages) == 0 {
			continue
		}

		outerMsg := response.Messages[0]
		outerData := json.RawMessage(*outerMsg.Body)

		msg := &snsMessage{}
		err = json.Unmarshal(outerData, &msg)
		if err != nil {
			return err
		}

		attributes := make(map[string]string)
		for attribute, value := range msg.MessageAttributes {
			if value.Type == "String" {
				attributes[attribute] = value.Value
			}
		}

		if err := handler(&Message{
			Data:       []byte(msg.Message),
			Attributes: attributes,
		}); err == nil {
			// acknowledge processing by deleting the message from queue
			// we can safely ignore delete errors because message processing is assumed to be idempotent
			conn.sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      &queueURL,
				ReceiptHandle: outerMsg.ReceiptHandle,
			})
		}
	}
}

func (conn *awsBroker) getQueueURL(subscriptionID string) (string, error) {
	queueURL, _ := conn.queueURLs[subscriptionID]
	if queueURL == "" {
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
	topicARN, _ := conn.topicARNs[topicID]

	if topicARN == "" {
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
		snsClient: sns.New(session),
		sqsClient: sqs.New(session),
		topicARNs: make(map[string]string),
		queueURLs: make(map[string]string),
	}

	return conn, nil
}
