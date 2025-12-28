package profiler

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/yourusername/s3-profiler/types"
)

// BucketAnalyzer handles bucket-level analysis
type BucketAnalyzer struct {
	s3Client *s3.Client
	limit    int64
}

// NewBucketAnalyzer creates a new bucket analyzer
func NewBucketAnalyzer(s3Client *s3.Client, limit int64) *BucketAnalyzer {
	return &BucketAnalyzer{
		s3Client: s3Client,
		limit:    limit,
	}
}

// AnalyzeBucket performs complete analysis of a bucket
func (ba *BucketAnalyzer) AnalyzeBucket(ctx context.Context, bucketName, region string) (*types.BucketSummary, []types.ObjectMetadata, error) {
	summary := &types.BucketSummary{
		Name:           bucketName,
		Region:         region,
		StorageClasses: make(map[string]types.StorageClassStats),
	}

	// Get bucket creation date
	creationDate, err := ba.getBucketCreationDate(ctx, bucketName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get bucket creation date: %w", err)
	}
	summary.CreationDate = creationDate

	// List and analyze objects
	objects, err := ba.listObjects(ctx, bucketName, summary)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list objects: %w", err)
	}

	// Calculate estimated cost
	summary.EstimatedCost = ba.calculateCost(summary.StorageClasses)

	return summary, objects, nil
}

// getBucketCreationDate retrieves the bucket creation date
func (ba *BucketAnalyzer) getBucketCreationDate(ctx context.Context, bucketName string) (time.Time, error) {
	// List all buckets to find the creation date
	result, err := ba.s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return time.Time{}, err
	}

	for _, bucket := range result.Buckets {
		if aws.ToString(bucket.Name) == bucketName {
			return aws.ToTime(bucket.CreationDate), nil
		}
	}

	return time.Time{}, fmt.Errorf("bucket %s not found", bucketName)
}

// listObjects lists all objects in the bucket and collects statistics
func (ba *BucketAnalyzer) listObjects(ctx context.Context, bucketName string, summary *types.BucketSummary) ([]types.ObjectMetadata, error) {
	var objects []types.ObjectMetadata
	var continuationToken *string
	processedCount := int64(0)

	for {
		// Check if we've reached the limit
		if ba.limit > 0 && processedCount >= ba.limit {
			fmt.Printf("Reached limit of %d objects\n", ba.limit)
			break
		}

		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucketName),
			ContinuationToken: continuationToken,
		}

		// Set max keys if limit is specified
		if ba.limit > 0 {
			remaining := ba.limit - processedCount
			if remaining < 1000 {
				input.MaxKeys = aws.Int32(int32(remaining))
			}
		}

		result, err := ba.s3Client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, err
		}

		// Process objects
		for _, obj := range result.Contents {
			key := aws.ToString(obj.Key)
			size := aws.ToInt64(obj.Size)
			storageClass := string(obj.StorageClass)
			if storageClass == "" {
				storageClass = "STANDARD"
			}

			// Update summary statistics
			summary.TotalObjects++
			summary.TotalSize += size

			// Update storage class stats
			stats := summary.StorageClasses[storageClass]
			stats.Count++
			stats.Size += size
			summary.StorageClasses[storageClass] = stats

			// Collect object metadata
			objects = append(objects, types.ObjectMetadata{
				Key:          key,
				Size:         size,
				LastModified: aws.ToTime(obj.LastModified),
				StorageClass: storageClass,
				ETag:         aws.ToString(obj.ETag),
			})

			processedCount++
		}

		// Show progress
		fmt.Printf("Processed %d objects...\n", processedCount)

		// Check if there are more results
		if !aws.ToBool(result.IsTruncated) {
			break
		}

		continuationToken = result.NextContinuationToken
	}

	return objects, nil
}

// calculateCost estimates monthly storage cost based on storage classes
func (ba *BucketAnalyzer) calculateCost(storageClasses map[string]types.StorageClassStats) float64 {
	// Pricing per GB per month (approximate US East)
	pricing := map[string]float64{
		"STANDARD":            0.023,
		"INTELLIGENT_TIERING": 0.023,
		"STANDARD_IA":         0.0125,
		"ONEZONE_IA":          0.01,
		"GLACIER":             0.004,
		"GLACIER_IR":          0.004,
		"DEEP_ARCHIVE":        0.00099,
	}

	totalCost := 0.0
	for class, stats := range storageClasses {
		sizeGB := float64(stats.Size) / (1024 * 1024 * 1024)
		if price, ok := pricing[class]; ok {
			totalCost += sizeGB * price
		} else {
			// Default to STANDARD pricing if unknown
			totalCost += sizeGB * pricing["STANDARD"]
		}
	}

	return totalCost
}

// ListAllBuckets returns a list of all bucket names
func ListAllBuckets(ctx context.Context, s3Client *s3.Client) ([]string, error) {
	result, err := s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	var buckets []string
	for _, bucket := range result.Buckets {
		buckets = append(buckets, aws.ToString(bucket.Name))
	}

	return buckets, nil
}
