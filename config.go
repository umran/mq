package mq

const (
	// ProviderGCP ...
	ProviderGCP = "gcloud"
	// ProviderAWS ...
	ProviderAWS = "aws"
)

// Config represents configuration options for the cloud provider.
// The Provider can currently be either "aws" or "gcloud".
// If the Provider is "aws", then AWSRegion must be set.
// If the Provider is "gcloud", then GCloudProject must be set.
type Config struct {
	// Provider represents the cloud provider.
	// Currently this can either be "aws" or "gcloud"
	Provider string

	// AWSRegion represents which AWS region to connect to
	AWSRegion string

	// GCloudProject represents which GCloud project to connect to
	GCloudProject string
}
