package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Client wraps the AWS S3 client with configuration
type Client struct {
	S3     *s3.Client
	Config aws.Config
}

// NewClient creates a new AWS S3 client with the specified profile and region
func NewClient(ctx context.Context, profile, region string) (*Client, error) {
	var opts []func(*config.LoadOptions) error

	// Add profile if specified
	if profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(profile))
	}

	// Add region if specified
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}

	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(cfg)

	return &Client{
		S3:     s3Client,
		Config: cfg,
	}, nil
}

// GetBucketRegion retrieves the region for a specific bucket
func (c *Client) GetBucketRegion(ctx context.Context, bucketName string) (string, error) {
	result, err := c.S3.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return "", err
	}

	// Handle empty region (means us-east-1)
	if result.LocationConstraint == "" {
		return "us-east-1", nil
	}

	return string(result.LocationConstraint), nil
}
