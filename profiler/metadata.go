package profiler

import (
	"path/filepath"
	"strings"

	"github.com/yourusername/s3-profiler/types"
)

// MetadataAnalyzer handles metadata analysis and aggregation
type MetadataAnalyzer struct{}

// NewMetadataAnalyzer creates a new metadata analyzer
func NewMetadataAnalyzer() *MetadataAnalyzer {
	return &MetadataAnalyzer{}
}

// AnalyzeMetadata performs metadata analysis on the collected objects
func (ma *MetadataAnalyzer) AnalyzeMetadata(objects []types.ObjectMetadata) *types.MetadataSummary {
	summary := &types.MetadataSummary{
		Objects:       objects,
		FileTypeStats: make(map[string]int64),
	}

	// Initialize date range
	if len(objects) > 0 {
		summary.DateRange.Earliest = objects[0].LastModified
		summary.DateRange.Latest = objects[0].LastModified
	}

	// Analyze each object
	for _, obj := range objects {
		// Extract file extension
		ext := ma.getFileExtension(obj.Key)
		summary.FileTypeStats[ext]++

		// Update date range
		if obj.LastModified.Before(summary.DateRange.Earliest) {
			summary.DateRange.Earliest = obj.LastModified
		}
		if obj.LastModified.After(summary.DateRange.Latest) {
			summary.DateRange.Latest = obj.LastModified
		}
	}

	// Generate size distribution histogram
	summary.SizeDistribution = ma.generateSizeDistribution(objects)

	return summary
}

// getFileExtension extracts the file extension from an object key
func (ma *MetadataAnalyzer) getFileExtension(key string) string {
	// Get the base filename
	base := filepath.Base(key)

	// Check if it has an extension
	ext := filepath.Ext(base)
	if ext == "" {
		// Check if it's a directory-like key (ends with /)
		if strings.HasSuffix(key, "/") {
			return "[directory]"
		}
		return "[no extension]"
	}

	// Return extension without the dot, in lowercase
	return strings.ToLower(strings.TrimPrefix(ext, "."))
}

// generateSizeDistribution creates a histogram of file sizes
func (ma *MetadataAnalyzer) generateSizeDistribution(objects []types.ObjectMetadata) []types.SizeBucket {
	buckets := []types.SizeBucket{
		{Label: "0-1KB", Min: 0, Max: 1024, Count: 0},
		{Label: "1KB-1MB", Min: 1024, Max: 1024 * 1024, Count: 0},
		{Label: "1MB-100MB", Min: 1024 * 1024, Max: 100 * 1024 * 1024, Count: 0},
		{Label: "100MB-1GB", Min: 100 * 1024 * 1024, Max: 1024 * 1024 * 1024, Count: 0},
		{Label: "1GB+", Min: 1024 * 1024 * 1024, Max: -1, Count: 0},
	}

	for _, obj := range objects {
		for i := range buckets {
			if buckets[i].Max == -1 {
				// Last bucket (1GB+)
				if obj.Size >= buckets[i].Min {
					buckets[i].Count++
					break
				}
			} else {
				if obj.Size >= buckets[i].Min && obj.Size < buckets[i].Max {
					buckets[i].Count++
					break
				}
			}
		}
	}

	return buckets
}
