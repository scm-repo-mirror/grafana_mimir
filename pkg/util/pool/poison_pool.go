// SPDX-License-Identifier: AGPL-3.0-only

//go:build poison_pools

package pool

// PoisonByte is the byte value used to fill buffers when they are returned to
// the pool. This pattern (0xDE) is chosen because it's recognizable in debug
// output and unlikely to appear in valid data.
const PoisonByte = 0xDE

// poisonPoolsEnabled indicates whether poison pool detection is active.
// When the poison_pools build tag is set, this is true.
const poisonPoolsEnabled = true

// PoisonByteSlice fills a byte slice with the poison pattern.
// This is called when buffers are returned to pools to detect use-after-free bugs.
func PoisonByteSlice(b []byte) {
	// Use the full capacity, not just the length
	b = b[:cap(b)]
	for i := range b {
		b[i] = PoisonByte
	}
}

// ContainsPoison checks if a byte slice contains poison bytes.
// Returns true if more than the threshold number of poison bytes are found,
// indicating a likely use-after-free bug.
func ContainsPoison(b []byte) bool {
	const threshold = 3 // Minimum consecutive poison bytes to trigger detection
	if len(b) < threshold {
		return false
	}

	poisonCount := 0
	for _, v := range b {
		if v == PoisonByte {
			poisonCount++
			if poisonCount >= threshold {
				return true
			}
		} else {
			poisonCount = 0
		}
	}
	return false
}

// CountPoisonBytes returns the total number of poison bytes in the slice.
func CountPoisonBytes(b []byte) int {
	count := 0
	for _, v := range b {
		if v == PoisonByte {
			count++
		}
	}
	return count
}
