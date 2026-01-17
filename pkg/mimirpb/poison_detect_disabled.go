// SPDX-License-Identifier: AGPL-3.0-only

//go:build !poison_pools

package mimirpb

// ValidateLabelsNotPoisoned is a no-op when poison pools are disabled.
// When built with the poison_pools tag, this function checks for use-after-free bugs.
func ValidateLabelsNotPoisoned([]LabelAdapter) {}

// ValidateTimeSeriesLabelsNotPoisoned is a no-op when poison pools are disabled.
func ValidateTimeSeriesLabelsNotPoisoned(*TimeSeries) {}

// ValidatePreallocTimeseriesNotPoisoned is a no-op when poison pools are disabled.
func ValidatePreallocTimeseriesNotPoisoned(*PreallocTimeseries) {}
