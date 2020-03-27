package mq

import "errors"

// Config ...
type Config struct {
	Provider      string
	AWSRegion     string
	GCloudProject string
}

// NewConnection ...
func NewConnection(config *Config) (Broker, error) {
	switch config.Provider {
	case "aws":
		return newAwsConnection(config.AWSRegion)
	case "gcloud":
		return newGcloudConnection(config.GCloudProject)
	default:
		return nil, errors.New("unrecognized provider")
	}
}
