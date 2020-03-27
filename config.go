package mq

// Config represents configuration information for the cloud environment.
// The Provider can currently be either "aws" or "gcloud".
// If the Provider is "aws", then AWSRegion must be set.
// If the Provider is "gcloud", then GCloudProject must be set.
type Config struct {
	Provider      string
	AWSRegion     string
	GCloudProject string
}
