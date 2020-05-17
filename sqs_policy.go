package mq

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

type sqsPolicy struct {
	Version   string
	ID        string `json:"Id"`
	Statement []json.RawMessage
}

type sqsPolicyStatement struct {
	Sid       string
	Effect    string
	Principal map[string]string
	Action    string
	Resource  string
	Condition map[string]map[string]string
}

func (policy *sqsPolicy) AddPermission(queueARN, topicARN string) error {
	exists := false
	for _, statementBytes := range policy.Statement {
		statement := new(sqsPolicyStatement)
		if err := json.Unmarshal(statementBytes, statement); err != nil {
			continue
		}

		if statement.Effect != "Allow" ||
			statement.Principal["AWS"] != "*" ||
			statement.Action != "SQS:SendMessage" ||
			statement.Resource != queueARN {
			continue
		}

		if statement.Condition["ArnEquals"] == nil {
			continue
		}

		if statement.Condition["ArnEquals"]["aws:SourceArn"] != topicARN {
			continue
		}

		exists = true
		break
	}

	if !exists {
		statement := newSqsPolicyStatement(queueARN, topicARN)
		statementBytes, _ := json.Marshal(statement)

		policy.Statement = append(policy.Statement, json.RawMessage(statementBytes))
	}

	return nil
}

func newSqsPolicyStatement(queueARN, topicARN string) *sqsPolicyStatement {
	return &sqsPolicyStatement{
		Sid:    fmt.Sprintf("Sid%s", strconv.FormatInt(time.Now().Unix(), 10)),
		Effect: "Allow",
		Principal: map[string]string{
			"AWS": "*",
		},
		Action:   "SQS:SendMessage",
		Resource: queueARN,
		Condition: map[string]map[string]string{
			"ArnEquals": {
				"aws:SourceArn": topicARN,
			},
		},
	}
}

func newSqsPolicy(queueARN string) *sqsPolicy {
	return &sqsPolicy{
		Version: "2012-10-17",
		ID:      fmt.Sprintf("%s/SQSDefaultPolicy", queueARN),
	}
}
