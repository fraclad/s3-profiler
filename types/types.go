package types

import "time"

// BucketSummary contains summary statistics for an S3 bucket
type BucketSummary struct {
	Name           string
	Region         string
	CreationDate   time.Time
	TotalObjects   int64
	TotalSize      int64
	StorageClasses map[string]StorageClassStats
	EstimatedCost  float64
}

// StorageClassStats holds count and size for a specific storage class
type StorageClassStats struct {
	Count int64
	Size  int64
}

// ObjectMetadata contains metadata for a single S3 object
type ObjectMetadata struct {
	Key          string
	Size         int64
	LastModified time.Time
	StorageClass string
	ETag         string
}

// MetadataSummary contains aggregated metadata statistics
type MetadataSummary struct {
	Objects          []ObjectMetadata
	FileTypeStats    map[string]int64
	SizeDistribution []SizeBucket
	DateRange        DateRange
}

// SizeBucket represents a size range in the distribution histogram
type SizeBucket struct {
	Label string
	Min   int64
	Max   int64
	Count int64
}

// DateRange represents the earliest and latest modification dates
type DateRange struct {
	Earliest time.Time
	Latest   time.Time
}

// Partition represents a detected partition pattern in S3 keys
type Partition struct {
	Prefix      string
	Pattern     string
	ObjectCount int64
	TotalSize   int64
	Examples    []string
}

// ProfileConfig holds configuration for the profiling operation
type ProfileConfig struct {
	BucketNames []string
	Profile     string
	Region      string
	Limit       int64
	OutputDir   string
	AllBuckets  bool
}
