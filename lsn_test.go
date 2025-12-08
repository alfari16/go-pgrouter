package dbresolver

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// TestLSNParsing tests LSN parsing and comparison functions
func TestLSNParsing(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedLSN LSN
		expectError bool
	}{
		{
			name:  "valid LSN 0/0",
			input: "0/0",
			expectedLSN: LSN{
				Upper: 0,
				Lower: 0,
			},
			expectError: false,
		},
		{
			name:  "valid LSN 0/3000060",
			input: "0/3000060",
			expectedLSN: LSN{
				Upper: 0,
				Lower: 0x3000060,
			},
			expectError: false,
		},
		{
			name:  "valid LSN with uppercase hex",
			input: "1/A0B1C2",
			expectedLSN: LSN{
				Upper: 1,
				Lower: 0xA0B1C2,
			},
			expectError: false,
		},
		{
			name:        "empty string",
			input:       "",
			expectError: true,
		},
		{
			name:        "invalid format",
			input:       "invalid",
			expectError: true,
		},
		{
			name:        "missing slash",
			input:       "3000060",
			expectError: true,
		},
		{
			name:        "invalid hex",
			input:       "0/XYZ",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lsn, err := ParseLSN(tt.input)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if lsn.Upper != tt.expectedLSN.Upper || lsn.Lower != tt.expectedLSN.Lower {
				t.Errorf("LSN mismatch: got {%X/%X}, want {%X/%X}",
					lsn.Upper, lsn.Lower,
					tt.expectedLSN.Upper, tt.expectedLSN.Lower)
			}

			// Test String() method
			str := lsn.String()
			if str != tt.input {
				t.Errorf("String() mismatch: got %s, want %s", str, tt.input)
			}
		})
	}
}

// TestLSNComparison tests LSN comparison functions
func TestLSNComparison(t *testing.T) {
	tests := []struct {
		name     string
		lsn1     LSN
		lsn2     LSN
		expected int
	}{
		{
			name:     "equal LSNs",
			lsn1:     LSN{Upper: 1, Lower: 0x1000},
			lsn2:     LSN{Upper: 1, Lower: 0x1000},
			expected: 0,
		},
		{
			name:     "lsn1 less than lsn2 (different upper)",
			lsn1:     LSN{Upper: 1, Lower: 0x1000},
			lsn2:     LSN{Upper: 2, Lower: 0x1000},
			expected: -1,
		},
		{
			name:     "lsn1 greater than lsn2 (different upper)",
			lsn1:     LSN{Upper: 2, Lower: 0x1000},
			lsn2:     LSN{Upper: 1, Lower: 0x1000},
			expected: 1,
		},
		{
			name:     "lsn1 less than lsn2 (same upper, different lower)",
			lsn1:     LSN{Upper: 1, Lower: 0x1000},
			lsn2:     LSN{Upper: 1, Lower: 0x2000},
			expected: -1,
		},
		{
			name:     "lsn1 greater than lsn2 (same upper, different lower)",
			lsn1:     LSN{Upper: 1, Lower: 0x2000},
			lsn2:     LSN{Upper: 1, Lower: 0x1000},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.lsn1.Compare(tt.lsn2)
			if result != tt.expected {
				t.Errorf("Compare() = %d, want %d", result, tt.expected)
			}

			// Test convenience methods
			switch tt.expected {
			case 0:
				if !tt.lsn1.Equals(tt.lsn2) {
					t.Error("Equals() returned false for equal LSNs")
				}
				if !tt.lsn1.LessThanOrEqual(tt.lsn2) {
					t.Error("LessThanOrEqual() returned false for equal LSNs")
				}
				if !tt.lsn1.GreaterThanOrEqual(tt.lsn2) {
					t.Error("GreaterThanOrEqual() returned false for equal LSNs")
				}
			case -1:
				if !tt.lsn1.LessThan(tt.lsn2) {
					t.Error("LessThan() returned false for less LSN")
				}
				if !tt.lsn1.LessThanOrEqual(tt.lsn2) {
					t.Error("LessThanOrEqual() returned false for less LSN")
				}
				if tt.lsn1.GreaterThan(tt.lsn2) {
					t.Error("GreaterThan() returned true for less LSN")
				}
			case 1:
				if !tt.lsn1.GreaterThan(tt.lsn2) {
					t.Error("GreaterThan() returned false for greater LSN")
				}
				if !tt.lsn1.GreaterThanOrEqual(tt.lsn2) {
					t.Error("GreaterThanOrEqual() returned false for greater LSN")
				}
				if tt.lsn1.LessThan(tt.lsn2) {
					t.Error("LessThan() returned true for greater LSN")
				}
			}
		})
	}
}

// TestLSNSubtract tests LSN subtraction
func TestLSNSubtract(t *testing.T) {
	tests := []struct {
		name     string
		lsn1     LSN
		lsn2     LSN
		expected uint64
	}{
		{
			name:     "same LSN",
			lsn1:     LSN{Upper: 1, Lower: 0x1000},
			lsn2:     LSN{Upper: 1, Lower: 0x1000},
			expected: 0,
		},
		{
			name:     "lsn1 greater by lower part",
			lsn1:     LSN{Upper: 1, Lower: 0x2000},
			lsn2:     LSN{Upper: 1, Lower: 0x1000},
			expected: 0x1000,
		},
		{
			name:     "lsn1 less than lsn2",
			lsn1:     LSN{Upper: 1, Lower: 0x1000},
			lsn2:     LSN{Upper: 1, Lower: 0x2000},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.lsn1.Subtract(tt.lsn2)
			if result != tt.expected {
				t.Errorf("Subtract() = %d, want %d", result, tt.expected)
			}
		})
	}
}

// TestLSNFromUint64 tests conversion between LSN and uint64
func TestLSNFromUint64(t *testing.T) {
	tests := []struct {
		name     string
		value    uint64
		expected LSN
	}{
		{
			name:     "zero",
			value:    0,
			expected: LSN{Upper: 0, Lower: 0},
		},
		{
			name:     "small value",
			value:    0x1234,
			expected: LSN{Upper: 0, Lower: 0x1234},
		},
		{
			name:     "value with upper part",
			value:    0x1000000000000,
			expected: LSN{Upper: 0x10000, Lower: 0},
		},
		{
			name:     "complex value",
			value:    0x123456789ABCDEF0,
			expected: LSN{Upper: 0x12345678, Lower: 0x9ABCDEF0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lsn := LSNFromUint64(tt.value)
			if lsn.Upper != tt.expected.Upper || lsn.Lower != tt.expected.Lower {
				t.Errorf("LSNFromUint64() = {%X/%X}, want {%X/%X}",
					lsn.Upper, lsn.Lower,
					tt.expected.Upper, tt.expected.Lower)
			}

			// Test reverse conversion
			backToUint64 := lsn.ToUint64()
			if backToUint64 != tt.value {
				t.Errorf("ToUint64() = %X, want %X", backToUint64, tt.value)
			}
		})
	}
}

// TestCausalConsistencyConfig tests default configuration
func TestCausalConsistencyConfig(t *testing.T) {
	config := DefaultCausalConsistencyConfig()

	if config.Enabled {
		t.Error("Default config should have Enabled=false")
	}

	if config.Level != ReadYourWrites {
		t.Errorf("Default level should be ReadYourWrites, got %v", config.Level)
	}

	if config.Timeout != 5*time.Second {
		t.Errorf("Default Timeout should be 5s, got %v", config.Timeout)
	}
}

// TestLSNContext tests context management for LSN
func TestLSNContext(t *testing.T) {
	ctx := context.Background()

	// Test that empty context returns nil
	lsnCtx := GetLSNContext(ctx)
	if lsnCtx != nil {
		t.Error("Expected nil LSN context from empty context")
	}

	// Test adding and retrieving LSN context
	requiredLSN := LSN{Upper: 1, Lower: 0x1000}
	testLSNCtx := &LSNContext{
		RequiredLSN: requiredLSN,
		Level:       ReadYourWrites,
		ForceMaster: false,
	}

	ctx = WithLSNContext(ctx, testLSNCtx)
	retrievedCtx := GetLSNContext(ctx)

	if retrievedCtx == nil {
		t.Error("Expected to retrieve LSN context")
	}

	if retrievedCtx.RequiredLSN.Upper != requiredLSN.Upper ||
		retrievedCtx.RequiredLSN.Lower != requiredLSN.Lower {
		t.Error("Retrieved LSN context doesn't match original")
	}

	if retrievedCtx.Level != ReadYourWrites {
		t.Error("Retrieved LSN context level doesn't match")
	}
}

// MockDB creates a mock database connection for testing
func MockDB() *sql.DB {
	// This would normally open a real connection, but for unit tests
	// we can use nil since most tests don't actually query the database
	return nil
}

// TestNewDBResolverWithLSN tests creating a new DB resolver with LSN features
func TestNewDBResolverWithLSN(t *testing.T) {
	primary := MockDB()
	replica := MockDB()

	// Test without LSN features
	db := New(WithPrimaryDBs(primary), WithReplicaDBs(replica))
	if db.IsCausalConsistencyEnabled() {
		t.Error("Causal consistency should be disabled by default")
	}

	// Test with LSN features enabled
	ccConfig := &CausalConsistencyConfig{
		Enabled:          true,
		Level:            ReadYourWrites,
		FallbackToMaster: true,
	}

	db = New(
		WithPrimaryDBs(primary),
		WithReplicaDBs(replica),
		WithCausalConsistencyConfig(ccConfig),
	)

	if !db.IsCausalConsistencyEnabled() {
		t.Error("Causal consistency should be enabled")
	}
}

// BenchmarkLSNParse benchmarks LSN parsing performance
func BenchmarkLSNParse(b *testing.B) {
	lsnStr := "1/A0B1C2D3E4F5"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseLSN(lsnStr)
	}
}

// BenchmarkLSNCompare benchmarks LSN comparison performance
func BenchmarkLSNCompare(b *testing.B) {
	lsn1 := LSN{Upper: 12345, Lower: 0xABCDEF00}
	lsn2 := LSN{Upper: 12346, Lower: 0x12345678}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = lsn1.Compare(lsn2)
	}
}
