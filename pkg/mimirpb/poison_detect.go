// SPDX-License-Identifier: AGPL-3.0-only

//go:build poison_pools

package mimirpb

import (
	"fmt"
	"unsafe"

	"github.com/grafana/mimir/pkg/util/pool"
)

// ValidateLabelsNotPoisoned checks if any label name or value contains poison bytes,
// indicating a use-after-free bug where label data is accessed after its backing
// buffer has been returned to the pool. If poisoned data is detected, this function
// panics with a detailed error message.
//
// This function is only active when built with the poison_pools build tag.
func ValidateLabelsNotPoisoned(ls []LabelAdapter) {
	for _, l := range ls {
		nameBytes := unsafe.Slice(unsafe.StringData(l.Name), len(l.Name))
		valueBytes := unsafe.Slice(unsafe.StringData(l.Value), len(l.Value))

		if pool.ContainsPoison(nameBytes) {
			panic(fmt.Sprintf("USE-AFTER-FREE: poisoned label name detected: %q (poison byte count: %d, len: %d)",
				l.Name, pool.CountPoisonBytes(nameBytes), len(l.Name)))
		}
		if pool.ContainsPoison(valueBytes) {
			panic(fmt.Sprintf("USE-AFTER-FREE: poisoned label value detected: name=%q value=%q (poison byte count: %d, len: %d)",
				l.Name, l.Value, pool.CountPoisonBytes(valueBytes), len(l.Value)))
		}
	}
}

// ValidateTimeSeriesLabelsNotPoisoned validates that none of the labels in a
// TimeSeries contain poison bytes.
func ValidateTimeSeriesLabelsNotPoisoned(ts *TimeSeries) {
	if ts == nil {
		return
	}
	ValidateLabelsNotPoisoned(ts.Labels)
	for _, e := range ts.Exemplars {
		ValidateLabelsNotPoisoned(e.Labels)
	}
}

// ValidatePreallocTimeseriesNotPoisoned validates that none of the labels in a
// PreallocTimeseries contain poison bytes.
func ValidatePreallocTimeseriesNotPoisoned(ts *PreallocTimeseries) {
	if ts == nil || ts.TimeSeries == nil {
		return
	}
	ValidateTimeSeriesLabelsNotPoisoned(ts.TimeSeries)
}
