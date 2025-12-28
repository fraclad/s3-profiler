package profiler

import (
	"regexp"
	"sort"
	"strings"

	"github.com/yourusername/s3-profiler/types"
)

// PartitionAnalyzer handles partition detection in S3 keys
type PartitionAnalyzer struct{}

// NewPartitionAnalyzer creates a new partition analyzer
func NewPartitionAnalyzer() *PartitionAnalyzer {
	return &PartitionAnalyzer{}
}

// AnalyzePartitions detects partitions in object keys
func (pa *PartitionAnalyzer) AnalyzePartitions(objects []types.ObjectMetadata) []types.Partition {
	if len(objects) == 0 {
		return nil
	}

	// Detect different types of partitions
	var partitions []types.Partition

	// 1. Detect date-based partitions
	datePartitions := pa.detectDatePartitions(objects)
	partitions = append(partitions, datePartitions...)

	// 2. Detect hierarchical prefix partitions (if no date partitions found)
	if len(datePartitions) == 0 {
		hierarchicalPartitions := pa.detectHierarchicalPartitions(objects)
		partitions = append(partitions, hierarchicalPartitions...)
	}

	return partitions
}

// detectDatePartitions detects date-based partition patterns
func (pa *PartitionAnalyzer) detectDatePartitions(objects []types.ObjectMetadata) []types.Partition {
	patterns := []struct {
		name  string
		regex *regexp.Regexp
	}{
		{"year=YYYY/month=MM/day=DD", regexp.MustCompile(`year=(\d{4})/month=(\d{2})/day=(\d{2})`)},
		{"year=YYYY/month=MM", regexp.MustCompile(`year=(\d{4})/month=(\d{2})`)},
		{"YYYY/MM/DD", regexp.MustCompile(`(\d{4})/(\d{2})/(\d{2})`)},
		{"YYYY/MM", regexp.MustCompile(`(\d{4})/(\d{2})`)},
		{"YYYY-MM-DD", regexp.MustCompile(`(\d{4})-(\d{2})-(\d{2})`)},
		{"dt=YYYY-MM-DD", regexp.MustCompile(`dt=(\d{4})-(\d{2})-(\d{2})`)},
	}

	for _, pattern := range patterns {
		partitions := pa.groupByPattern(objects, pattern.name, pattern.regex)
		if len(partitions) > 0 {
			// Check if this pattern covers a significant portion of objects
			totalMatched := int64(0)
			for _, p := range partitions {
				totalMatched += p.ObjectCount
			}

			// If pattern covers >50% of objects, consider it valid
			if float64(totalMatched)/float64(len(objects)) > 0.5 {
				return partitions
			}
		}
	}

	return nil
}

// groupByPattern groups objects by a regex pattern
func (pa *PartitionAnalyzer) groupByPattern(objects []types.ObjectMetadata, patternName string, regex *regexp.Regexp) []types.Partition {
	partitionMap := make(map[string]*types.Partition)

	for _, obj := range objects {
		matches := regex.FindStringSubmatch(obj.Key)
		if len(matches) > 0 {
			// Extract the matched prefix
			prefix := matches[0]

			if partition, exists := partitionMap[prefix]; exists {
				partition.ObjectCount++
				partition.TotalSize += obj.Size
				if len(partition.Examples) < 3 {
					partition.Examples = append(partition.Examples, obj.Key)
				}
			} else {
				partitionMap[prefix] = &types.Partition{
					Prefix:      prefix,
					Pattern:     patternName,
					ObjectCount: 1,
					TotalSize:   obj.Size,
					Examples:    []string{obj.Key},
				}
			}
		}
	}

	// Convert map to slice and sort by prefix
	var partitions []types.Partition
	for _, p := range partitionMap {
		partitions = append(partitions, *p)
	}

	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].Prefix < partitions[j].Prefix
	})

	return partitions
}

// detectHierarchicalPartitions detects partitions based on common prefixes
func (pa *PartitionAnalyzer) detectHierarchicalPartitions(objects []types.ObjectMetadata) []types.Partition {
	prefixMap := make(map[string]*types.Partition)

	for _, obj := range objects {
		// Extract top-level prefix (first part before /)
		parts := strings.Split(obj.Key, "/")
		if len(parts) > 1 {
			prefix := parts[0]

			if partition, exists := prefixMap[prefix]; exists {
				partition.ObjectCount++
				partition.TotalSize += obj.Size
				if len(partition.Examples) < 3 {
					partition.Examples = append(partition.Examples, obj.Key)
				}
			} else {
				prefixMap[prefix] = &types.Partition{
					Prefix:      prefix + "/",
					Pattern:     "hierarchical (top-level prefix)",
					ObjectCount: 1,
					TotalSize:   obj.Size,
					Examples:    []string{obj.Key},
				}
			}
		}
	}

	// Only return if we found meaningful partitions (more than 1)
	if len(prefixMap) <= 1 {
		return nil
	}

	// Convert map to slice and sort by object count (descending)
	var partitions []types.Partition
	for _, p := range prefixMap {
		partitions = append(partitions, *p)
	}

	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].ObjectCount > partitions[j].ObjectCount
	})

	return partitions
}
