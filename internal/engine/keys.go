package engine

import (
	"sort"

	"github.com/lisama569/pipeline-observability-engine-go/internal/model"
)

// sortedStatsKeys returns aggregation keys in lexicographic order so all
// derived outputs (conditions, dumps) are stable and reproducible.
func sortedStatsKeys(m map[string]model.OperatorStats) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
