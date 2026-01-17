// SPDX-License-Identifier: AGPL-3.0-only

//go:build !poison_pools

package pool

// PoisonByte is the byte value used to fill buffers when they are returned to
// the pool. This constant is provided for consistency but is not used when
// poison pools are disabled.
const PoisonByte = 0xDE

// poisonPoolsEnabled indicates whether poison pool detection is active.
// When the poison_pools build tag is NOT set, this is false and all
// poison-related functions are no-ops for zero runtime overhead.
const poisonPoolsEnabled = false

// PoisonByteSlice is a no-op when poison pools are disabled.
func PoisonByteSlice([]byte) {}

// ContainsPoison always returns false when poison pools are disabled.
func ContainsPoison([]byte) bool { return false }

// CountPoisonBytes always returns 0 when poison pools are disabled.
func CountPoisonBytes([]byte) int { return 0 }
