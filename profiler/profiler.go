package profiler

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/yourusername/s3-profiler/output"
)

// Profiler orchestrates the profiling of S3 buckets
type Profiler struct {
	s3Client          *s3.Client
	bucketAnalyzer    *BucketAnalyzer
	metadataAnalyzer  *MetadataAnalyzer
	partitionAnalyzer *PartitionAnalyzer
	writer            *output.Writer
}

// NewProfiler creates a new profiler instance
func NewProfiler(s3Client *s3.Client, outputDir string, limit int64) *Profiler {
	return &Profiler{
		s3Client:          s3Client,
		bucketAnalyzer:    NewBucketAnalyzer(s3Client, limit),
		metadataAnalyzer:  NewMetadataAnalyzer(),
		partitionAnalyzer: NewPartitionAnalyzer(),
		writer:            output.NewWriter(outputDir),
	}
}

// ProfileBucket profiles a single S3 bucket
func (p *Profiler) ProfileBucket(ctx context.Context, bucketName, region string) error {
	fmt.Printf("\n%s\n", output.FormatHeader(fmt.Sprintf("Profiling bucket: %s", bucketName)))

	// Step 1: Analyze bucket
	fmt.Println("Step 1/4: Analyzing bucket and listing objects...")
	summary, objects, err := p.bucketAnalyzer.AnalyzeBucket(ctx, bucketName, region)
	if err != nil {
		return fmt.Errorf("failed to analyze bucket: %w", err)
	}
	fmt.Printf("Found %d objects (Total size: %s)\n", summary.TotalObjects, output.FormatBytes(summary.TotalSize))

	// Step 2: Analyze metadata
	fmt.Println("\nStep 2/4: Analyzing metadata...")
	metadataSummary := p.metadataAnalyzer.AnalyzeMetadata(objects)
	fmt.Printf("Identified %d file types\n", len(metadataSummary.FileTypeStats))

	// Step 3: Detect partitions
	fmt.Println("\nStep 3/4: Detecting partitions...")
	partitions := p.partitionAnalyzer.AnalyzePartitions(objects)
	if len(partitions) > 0 {
		fmt.Printf("Detected %d partition(s)\n", len(partitions))
	} else {
		fmt.Println("No partitions detected")
	}

	// Step 4: Write output files
	fmt.Println("\nStep 4/4: Writing output files...")

	if err := p.writer.WriteBucketSummary(summary); err != nil {
		return fmt.Errorf("failed to write bucket summary: %w", err)
	}
	fmt.Printf("  - %s-summary.txt\n", bucketName)

	if err := p.writer.WriteMetadataSummary(bucketName, metadataSummary); err != nil {
		return fmt.Errorf("failed to write metadata summary: %w", err)
	}
	fmt.Printf("  - %s-metadata.txt\n", bucketName)

	if err := p.writer.WritePartitions(bucketName, partitions); err != nil {
		return fmt.Errorf("failed to write partitions: %w", err)
	}
	fmt.Printf("  - %s-partitions.txt\n", bucketName)

	fmt.Printf("\n%s Profiling completed successfully!\n\n", "✓")

	return nil
}

// ProfileMultipleBuckets profiles multiple S3 buckets
func (p *Profiler) ProfileMultipleBuckets(ctx context.Context, bucketNames []string, getRegion func(context.Context, string) (string, error)) error {
	totalBuckets := len(bucketNames)
	successCount := 0
	failedBuckets := []string{}

	fmt.Printf("Profiling %d bucket(s)...\n", totalBuckets)

	for i, bucketName := range bucketNames {
		fmt.Printf("\n[%d/%d] Processing bucket: %s\n", i+1, totalBuckets, bucketName)

		// Get bucket region
		region, err := getRegion(ctx, bucketName)
		if err != nil {
			fmt.Printf("ERROR: Failed to get region for bucket %s: %v\n", bucketName, err)
			failedBuckets = append(failedBuckets, bucketName)
			continue
		}

		// Profile the bucket
		if err := p.ProfileBucket(ctx, bucketName, region); err != nil {
			fmt.Printf("ERROR: Failed to profile bucket %s: %v\n", bucketName, err)
			failedBuckets = append(failedBuckets, bucketName)
			continue
		}

		successCount++
	}

	// Print summary
	fmt.Printf("\n%s\n", output.FormatHeader("Summary"))
	fmt.Printf("Total buckets: %d\n", totalBuckets)
	fmt.Printf("Successfully profiled: %d\n", successCount)
	fmt.Printf("Failed: %d\n", len(failedBuckets))

	if len(failedBuckets) > 0 {
		fmt.Println("\nFailed buckets:")
		for _, bucket := range failedBuckets {
			fmt.Printf("  - %s\n", bucket)
		}
	}

	return nil
}
