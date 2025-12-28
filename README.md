# S3 Profiler

A Go CLI tool for profiling AWS S3 buckets, generating comprehensive reports including bucket summaries, metadata analysis, and partition detection.

## Features

- Analyze single or multiple S3 buckets
- Generate three detailed report files per bucket:
  - Bucket summary with storage class breakdown and cost estimates
  - Metadata summary with file type distribution and size analysis
  - Partition detection for organized data structures
- Support for large buckets with configurable object limits
- AWS credential chain support with optional profile selection

## Installation

### Build from source

```bash
go build -o s3-profiler
```

### Run directly

```bash
go run main.go [flags]
```

## Usage

### Basic usage

Profile a single bucket:
```bash
./s3-profiler --buckets my-bucket-name
```

Profile multiple buckets:
```bash
./s3-profiler --buckets bucket1,bucket2,bucket3
```

Profile all accessible buckets (with confirmation):
```bash
./s3-profiler --all
```

### Advanced options

Use a specific AWS profile:
```bash
./s3-profiler --buckets my-bucket --profile production
```

Limit the number of objects scanned per bucket:
```bash
./s3-profiler --buckets my-bucket --limit 10000
```

Specify output directory:
```bash
./s3-profiler --buckets my-bucket --output-dir ./reports
```

Specify AWS region:
```bash
./s3-profiler --buckets my-bucket --region us-west-2
```

## AWS Credentials

The tool uses the standard AWS credential chain:
1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
2. Shared credentials file (~/.aws/credentials)
3. IAM role (when running on EC2, ECS, etc.)

You can specify a named profile with the `--profile` flag.

## Required AWS Permissions

The tool requires the following S3 permissions:
- s3:ListAllMyBuckets (for --all flag)
- s3:ListBucket
- s3:GetBucketLocation
- s3:GetObject (metadata only)

Example IAM policy:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListAllMyBuckets",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "*"
    }
  ]
}
```

## Output Files

### bucket-name-summary.txt
Contains:
- Bucket name, region, and creation date
- Total object count and size
- Storage class breakdown with percentages
- Estimated monthly storage cost

### bucket-name-metadata.txt
Contains:
- File type distribution (top file extensions)
- Size distribution histogram
- Date range (earliest and latest modified dates)
- Object listing (sample for large buckets)

### bucket-name-partitions.txt
Contains:
- Detected partition patterns (date-based or hierarchical)
- Object count and size per partition
- Example keys for each partition

## Examples

### Example 1: Profile a data lake bucket
```bash
./s3-profiler \
  --buckets my-data-lake \
  --limit 100000 \
  --output-dir ./data-lake-reports
```

### Example 2: Profile multiple buckets with specific AWS profile
```bash
./s3-profiler \
  --buckets logs-bucket,analytics-bucket,backups-bucket \
  --profile production \
  --output-dir ./bucket-reports
```

### Example 3: Quick scan with object limit
```bash
./s3-profiler \
  --buckets my-bucket \
  --limit 1000 \
  --output-dir ./quick-scan
```

## Project Structure

```
s3-profiler/
├── main.go              # Entry point
├── go.mod               # Go module definition
├── go.sum               # Dependency checksums
├── types/
│   └── types.go         # Shared type definitions
├── aws/
│   └── client.go        # AWS S3 client wrapper
├── cmd/
│   └── root.go          # CLI command setup with Cobra
├── profiler/
│   ├── profiler.go      # Main orchestrator
│   ├── bucket.go        # Bucket analysis logic
│   ├── metadata.go      # Metadata collection and aggregation
│   └── partition.go     # Partition detection logic
└── output/
    ├── formatter.go     # Text formatting utilities
    └── writer.go        # Output file generation
```

## Development

### Run tests
```bash
go test ./...
```

### Format code
```bash
go fmt ./...
```

### Build for different platforms
```bash
# Linux
GOOS=linux GOARCH=amd64 go build -o s3-profiler-linux

# macOS
GOOS=darwin GOARCH=amd64 go build -o s3-profiler-macos

# Windows
GOOS=windows GOARCH=amd64 go build -o s3-profiler.exe
```

## Updating Module Path

When you connect this to GitHub, update the module path in go.mod:

1. Replace `github.com/yourusername/s3-profiler` with your actual GitHub repository path
2. Update imports in all Go files
3. Run `go mod tidy`

Example:
```bash
# In go.mod, change:
module github.com/yourusername/s3-profiler

# To:
module github.com/your-github-username/s3-profiler
```

Then update all imports in the source files and run:
```bash
go mod tidy
```

## License

MIT License

## Contributing

Contributions are welcome. Please open an issue or submit a pull request.
