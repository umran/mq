package mq

import "errors"

// Config ...
type Config struct {
	Provider      string
	AWSRegion     string
	GCloudProject string
}

// NewBroker ...
func NewBroker(config *Config) (Broker, error) {
	switch config.Provider {
	case "aws":
		return newAwsBroker(config.AWSRegion)
	case "gcloud":
		return newGcloudBroker(config.GCloudProject)
	default:
		return nil, errors.New("unrecognized provider")
	}
}
