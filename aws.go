package mq

import (
	"encoding/json"
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// AWSConnection ...
type AWSConnection struct {
	snsClient *sns.SNS
	sqsClient *sqs.SQS
	topicARNs map[string]string
	queueURLs map[string]string
}

// Publish ...
func (conn *AWSConnection) Publish(topicID string, message *Message) error {
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
func (conn *AWSConnection) Subscribe(subsctiptionID string, handler func(*Message) error) error {
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

		if len(response.Messages) > 0 {
			outerMsg := response.Messages[0]
			outerData, err := json.RawMessage(*outerMsg.Body).MarshalJSON()
			if err != nil {
				return err
			}

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
}

func (conn *AWSConnection) getQueueURL(topicID string) (string, error) {
	queueURL, _ := conn.queueURLs[topicID]
	if queueURL == "" {
		queueURLResult, err := conn.sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{
			QueueName: aws.String(topicID),
		})

		if err != nil {
			return queueURL, err
		}

		queueURL = *queueURLResult.QueueUrl
		conn.queueURLs[topicID] = queueURL
	}

	return queueURL, nil
}

func (conn *AWSConnection) getTopicARN(topicID string) (string, error) {
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
				topicAttributes, err := conn.snsClient.GetTopicAttributes(&sns.GetTopicAttributesInput{
					TopicArn: topic.TopicArn,
				})

				if err != nil {
					return topicARN, err
				}

				displayName, _ := topicAttributes.Attributes["DisplayName"]
				if displayName != nil && *displayName == topicID {
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

// NewAWSConnection ...
func NewAWSConnection(region string) (*AWSConnection, error) {
	session, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)

	if err != nil {
		return nil, err
	}

	conn := &AWSConnection{
		snsClient: sns.New(session),
		sqsClient: sqs.New(session),
		topicARNs: make(map[string]string),
		queueURLs: make(map[string]string),
	}

	return conn, nil
}
