package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	awsclient "github.com/yourusername/s3-profiler/aws"
	"github.com/yourusername/s3-profiler/profiler"
)

var (
	bucketNames string
	profile     string
	region      string
	limit       int64
	outputDir   string
	allBuckets  bool
)

// rootCmd represents the base command
var rootCmd = &cobra.Command{
	Use:   "s3-profiler",
	Short: "Profile AWS S3 buckets and generate detailed reports",
	Long: `s3-profiler is a CLI tool that analyzes AWS S3 buckets and generates
comprehensive reports including bucket summaries, metadata analysis, and partition detection.

The tool generates three output files per bucket:
  - bucket-name-summary.txt: Bucket statistics and storage class breakdown
  - bucket-name-metadata.txt: Object metadata and file type distribution
  - bucket-name-partitions.txt: Detected partition patterns`,
	RunE: runProfiler,
}

// Execute runs the root command
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.Flags().StringVarP(&bucketNames, "buckets", "b", "", "Comma-separated list of bucket names to profile")
	rootCmd.Flags().StringVarP(&profile, "profile", "p", "", "AWS profile name to use")
	rootCmd.Flags().StringVarP(&region, "region", "r", "", "AWS region (defaults to bucket region)")
	rootCmd.Flags().Int64VarP(&limit, "limit", "l", 0, "Maximum number of objects to scan per bucket (0 = unlimited)")
	rootCmd.Flags().StringVarP(&outputDir, "output-dir", "o", ".", "Directory for output files")
	rootCmd.Flags().BoolVarP(&allBuckets, "all", "a", false, "Profile all accessible buckets")
}

func runProfiler(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	// Create AWS client
	client, err := awsclient.NewClient(ctx, profile, region)
	if err != nil {
		return fmt.Errorf("failed to create AWS client: %w", err)
	}

	// Determine which buckets to profile
	var bucketsToProfile []string

	if bucketNames != "" {
		// Use specified buckets
		bucketsToProfile = strings.Split(bucketNames, ",")
		for i := range bucketsToProfile {
			bucketsToProfile[i] = strings.TrimSpace(bucketsToProfile[i])
		}
	} else if allBuckets {
		// List all buckets
		fmt.Println("Listing all accessible buckets...")
		bucketsToProfile, err = profiler.ListAllBuckets(ctx, client.S3)
		if err != nil {
			return fmt.Errorf("failed to list buckets: %w", err)
		}
		fmt.Printf("Found %d bucket(s)\n", len(bucketsToProfile))
	} else {
		// Default to all buckets with confirmation
		fmt.Println("No buckets specified. Listing all accessible buckets...")
		bucketsToProfile, err = profiler.ListAllBuckets(ctx, client.S3)
		if err != nil {
			return fmt.Errorf("failed to list buckets: %w", err)
		}

		fmt.Printf("\nFound %d bucket(s):\n", len(bucketsToProfile))
		for _, bucket := range bucketsToProfile {
			fmt.Printf("  - %s\n", bucket)
		}

		// Ask for confirmation
		fmt.Print("\nDo you want to profile all these buckets? (yes/no): ")
		reader := bufio.NewReader(os.Stdin)
		response, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		response = strings.ToLower(strings.TrimSpace(response))
		if response != "yes" && response != "y" {
			fmt.Println("Profiling cancelled.")
			return nil
		}
	}

	if len(bucketsToProfile) == 0 {
		fmt.Println("No buckets to profile.")
		return nil
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Create profiler
	p := profiler.NewProfiler(client.S3, outputDir, limit)

	// Profile buckets
	if len(bucketsToProfile) == 1 {
		// Single bucket
		bucketName := bucketsToProfile[0]
		bucketRegion, err := client.GetBucketRegion(ctx, bucketName)
		if err != nil {
			return fmt.Errorf("failed to get bucket region: %w", err)
		}
		return p.ProfileBucket(ctx, bucketName, bucketRegion)
	} else {
		// Multiple buckets
		return p.ProfileMultipleBuckets(ctx, bucketsToProfile, client.GetBucketRegion)
	}
}
