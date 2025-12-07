package dbresolver

import (
	"fmt"
	"strconv"
	"strings"
)

// LSN represents a PostgreSQL Log Sequence Number in the format X/Y
// where X is the log file ID and Y is the byte offset within the file
type LSN struct {
	Upper uint32 // Higher 32 bits (log file ID)
	Lower uint32 // Lower 32 bits (byte offset)
}

// ParseLSN parses a PostgreSQL LSN string in the format "X/Y"
// For example: "0/3000060", "1/A0B1C2"
func ParseLSN(lsnStr string) (LSN, error) {
	if lsnStr == "" {
		return LSN{}, fmt.Errorf("empty LSN string")
	}

	parts := strings.Split(lsnStr, "/")
	if len(parts) != 2 {
		return LSN{}, fmt.Errorf("invalid LSN format: %s (expected X/Y)", lsnStr)
	}

	upper, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return LSN{}, fmt.Errorf("invalid upper part of LSN: %s", parts[0])
	}

	lower, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return LSN{}, fmt.Errorf("invalid lower part of LSN: %s", parts[1])
	}

	return LSN{
		Upper: uint32(upper),
		Lower: uint32(lower),
	}, nil
}

// String returns the string representation of the LSN in PostgreSQL format X/Y
func (lsn LSN) String() string {
	return fmt.Sprintf("%X/%X", lsn.Upper, lsn.Lower)
}

// Compare compares this LSN with another LSN
// Returns:
//
//	-1 if this LSN < other LSN
//	 0 if this LSN == other LSN
//	 1 if this LSN > other LSN
func (lsn LSN) Compare(other LSN) int {
	if lsn.Upper < other.Upper {
		return -1
	}
	if lsn.Upper > other.Upper {
		return 1
	}
	// Upper parts are equal, compare lower parts
	if lsn.Lower < other.Lower {
		return -1
	}
	if lsn.Lower > other.Lower {
		return 1
	}
	return 0
}

// LessThan returns true if this LSN is less than the other LSN
func (lsn LSN) LessThan(other LSN) bool {
	return lsn.Compare(other) < 0
}

// LessThanOrEqual returns true if this LSN is less than or equal to the other LSN
func (lsn LSN) LessThanOrEqual(other LSN) bool {
	return lsn.Compare(other) <= 0
}

// GreaterThan returns true if this LSN is greater than the other LSN
func (lsn LSN) GreaterThan(other LSN) bool {
	return lsn.Compare(other) > 0
}

// GreaterThanOrEqual returns true if this LSN is greater than or equal to the other LSN
func (lsn LSN) GreaterThanOrEqual(other LSN) bool {
	return lsn.Compare(other) >= 0
}

// Equals returns true if this LSN is equal to the other LSN
func (lsn LSN) Equals(other LSN) bool {
	return lsn.Compare(other) == 0
}

// IsZero returns true if this LSN represents 0/0 (initial state)
func (lsn LSN) IsZero() bool {
	return lsn.Upper == 0 && lsn.Lower == 0
}

// Subtract calculates the difference in bytes between two LSNs
// Returns the number of bytes between this LSN and the other LSN
// If other LSN is greater than this LSN, returns 0
func (lsn LSN) Subtract(other LSN) uint64 {
	if lsn.LessThan(other) {
		return 0
	}

	// Convert LSNs to 64-bit integers for calculation
	thisUint64 := (uint64(lsn.Upper) << 32) | uint64(lsn.Lower)
	otherUint64 := (uint64(other.Upper) << 32) | uint64(other.Lower)

	return thisUint64 - otherUint64
}

// Add adds the specified number of bytes to this LSN and returns a new LSN
func (lsn LSN) Add(bytes uint64) LSN {
	// Convert LSN to 64-bit integer
	current := (uint64(lsn.Upper) << 32) | uint64(lsn.Lower)
	new := current + bytes

	// Convert back to LSN
	return LSN{
		Upper: uint32(new >> 32),
		Lower: uint32(new & 0xFFFFFFFF),
	}
}

// LSNFromUint64 creates an LSN from a 64-bit integer representation
func LSNFromUint64(value uint64) LSN {
	return LSN{
		Upper: uint32(value >> 32),
		Lower: uint32(value & 0xFFFFFFFF),
	}
}

// ToUint64 converts LSN to its 64-bit integer representation
func (lsn LSN) ToUint64() uint64 {
	return (uint64(lsn.Upper) << 32) | uint64(lsn.Lower)
}

// Constants for common PostgreSQL LSN functions
const (
	// PostgreSQL function to get current WAL LSN from master
	PGCurrentWALLSN = "pg_current_wal_lsn()"

	// PostgreSQL function to get last replay LSN from replica
	PGLastWalReplayLSN = "pg_last_wal_replay_lsn()"

	// PostgreSQL function to get WAL flush LSN
	PGWalFlushLSN = "pg_wal_lsn_diff(%s, %s)"
)
