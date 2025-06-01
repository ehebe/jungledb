package jungledb

import (
	"bytes" // For bytes.Equal
	"fmt"
	"os"
	"testing"
)

// TestMain cleans up test files before and after running tests.
func TestMain(m *testing.M) {
	// Clean up previous test files
	os.RemoveAll("testdata")
	os.MkdirAll("testdata", 0755)

	code := m.Run()

	// Clean up after tests
	os.RemoveAll("testdata")
	os.Exit(code)
}

// TestHsetHget tests the Hset and Hget operations with byte slices.
func TestHsetHget(t *testing.T) {
	db, err := Open("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	key := "user:1"
	field := "name"
	value := []byte("Alice") // Value is now []byte

	// Set value
	if err := db.Hset(key, field, value); err != nil {
		t.Fatalf("Hset failed: %v", err)
	}

	// Get value
	result, err := db.Hget(key, field)
	if err != nil {
		t.Fatalf("Hget failed: %v", err)
	}

	if !bytes.Equal(result, value) { // Use bytes.Equal for []byte comparison
		t.Errorf("value mismatch: expected %q, got %q", value, result)
	}

	// Test getting a non-existent field
	nonExistentResult, err := db.Hget(key, "non_existent_field")
	if err != nil {
		t.Fatalf("Hget for non-existent field failed: %v", err)
	}
	if nonExistentResult != nil {
		t.Errorf("expected nil for non-existent field, got %q", nonExistentResult)
	}

	// Test getting from a non-existent key
	nonExistentKeyResult, err := db.Hget("non_existent_key", "any_field")
	if err != nil {
		t.Fatalf("Hget for non-existent key failed: %v", err)
	}
	if nonExistentKeyResult != nil {
		t.Errorf("expected nil for non-existent key, got %q", nonExistentKeyResult)
	}
}

// TestHmsetHmget tests the Hmset and Hmget operations with byte slices.
func TestHmsetHmget(t *testing.T) {
	db, err := Open("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	key := "user:2"
	fields := map[string][]byte{ // Values are now []byte
		"name":  []byte("Bob"),
		"age":   []byte("30"),
		"email": []byte("bob@example.com"),
	}

	// Batch set
	if err := db.Hmset(key, fields); err != nil {
		t.Fatalf("Hmset failed: %v", err)
	}

	// Batch get
	keysToGet := []string{"name", "age", "email", "non_existent_field"} // Added a non-existent field
	values, err := db.Hmget(key, keysToGet)
	if err != nil {
		t.Fatalf("Hmget failed: %v", err)
	}

	if len(values) != len(keysToGet) {
		t.Errorf("returned value count mismatch: expected %d, got %d", len(keysToGet), len(values))
	}

	for i, k := range keysToGet {
		expectedValue, exists := fields[k]
		if !exists {
			// For non-existent fields, Hmget should return nil
			if values[i] != nil {
				t.Errorf("value mismatch for non-existent field %q: expected nil, got %q", k, values[i])
			}
			continue
		}

		if !bytes.Equal(values[i], expectedValue) {
			t.Errorf("value mismatch: field %q expected %q, got %q", k, expectedValue, values[i])
		}
	}

	// Test getting from a non-existent key
	nonExistentKeyValues, err := db.Hmget("non_existent_key_for_hmget", []string{"f1", "f2"})
	if err != nil {
		t.Fatalf("Hmget for non-existent key failed: %v", err)
	}
	if len(nonExistentKeyValues) != 2 || nonExistentKeyValues[0] != nil || nonExistentKeyValues[1] != nil {
		t.Errorf("expected slice of nils for non-existent key, got %v", nonExistentKeyValues)
	}
}

// TestHincrHgetInt tests the Hincr and HgetInt operations with binary integers.
func TestHincrHgetInt(t *testing.T) {
	db, err := Open("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	key := "counter"
	field := "value"

	// Hincr on a non-existent field should initialize to delta
	newValue, err := db.Hincr(key, field, 10)
	if err != nil {
		t.Fatalf("Hincr initial failed: %v", err)
	}
	if newValue != 10 {
		t.Errorf("initial Hincr value mismatch: expected 10, got %d", newValue)
	}

	// Get integer value
	value, err := db.HgetInt(key, field)
	if err != nil {
		t.Fatalf("HgetInt failed: %v", err)
	}
	if value != 10 {
		t.Errorf("initial HgetInt value mismatch: expected 10, got %d", value)
	}

	// Increment by 5
	newValue, err = db.Hincr(key, field, 5)
	if err != nil {
		t.Fatalf("Hincr failed: %v", err)
	}
	if newValue != 15 {
		t.Errorf("incremented value mismatch: expected 15, got %d", newValue)
	}

	// Decrement by 7
	newValue, err = db.Hincr(key, field, -7)
	if err != nil {
		t.Fatalf("Hincr failed: %v", err)
	}
	if newValue != 8 {
		t.Errorf("decremented value mismatch: expected 8, got %d", newValue)
	}

	// Test overflow (assuming int64, check limits if needed)
	// For simplicity, let's just try to hit a large number close to max int64
	maxInt64 := int64(^uint64(0) >> 1) // Max int64
	// Set value close to max
	_, err = db.Hincr(key, "overflow_field", maxInt64-100)
	if err != nil {
		t.Fatalf("Hincr setup for overflow failed: %v", err)
	}
	_, err = db.Hincr(key, "overflow_field", 200) // This should overflow
	if err == nil {
		t.Error("Hincr should have returned an overflow error")
	}
	if err != nil && err.Error() != "integer overflow" {
		t.Errorf("expected integer overflow error, got: %v", err)
	}

	// Test HgetInt on a non-existent field
	nonExistentIntValue, err := db.HgetInt(key, "non_existent_int_field")
	if err != nil {
		t.Fatalf("HgetInt for non-existent field failed: %v", err)
	}
	if nonExistentIntValue != 0 {
		t.Errorf("expected 0 for non-existent integer field, got %d", nonExistentIntValue)
	}
}

// TestHhasKey tests the HhasKey operation.
func TestHhasKey(t *testing.T) {
	db, err := Open("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	key := "exists_test"
	existingField := "present"
	missingField := "absent"

	// Set an existing field
	if err := db.Hset(key, existingField, []byte("value")); err != nil {
		t.Fatalf("Hset failed: %v", err)
	}

	// Check existing field
	exists, err := db.HhasKey(key, existingField)
	if err != nil {
		t.Fatalf("HhasKey failed: %v", err)
	}
	if !exists {
		t.Errorf("field %q should exist", existingField)
	}

	// Check non-existing field
	exists, err = db.HhasKey(key, missingField)
	if err != nil {
		t.Fatalf("HhasKey failed: %v", err)
	}
	if exists {
		t.Errorf("field %q should not exist", missingField)
	}

	// Check field in a non-existent key
	exists, err = db.HhasKey("non_existent_key_for_haskey", "any_field")
	if err != nil {
		t.Fatalf("HhasKey for non-existent key failed: %v", err)
	}
	if exists {
		t.Errorf("field in non-existent key should not exist")
	}
}

// TestHdel tests the Hdel operation.
func TestHdel(t *testing.T) {
	db, err := Open("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	key := "delete_test"
	field := "to_delete"
	nonExistentField := "non_existent_to_delete"

	// Set value
	if err := db.Hset(key, field, []byte("value")); err != nil {
		t.Fatalf("Hset failed: %v", err)
	}

	// Delete field
	if err := db.Hdel(key, field); err != nil {
		t.Fatalf("Hdel failed: %v", err)
	}

	// Check if field exists
	exists, err := db.HhasKey(key, field)
	if err != nil {
		t.Fatalf("HhasKey failed: %v", err)
	}
	if exists {
		t.Errorf("field %q should be deleted", field)
	}

	// Try deleting a non-existent field
	if err := db.Hdel(key, nonExistentField); err != nil {
		t.Fatalf("Hdel of non-existent field failed: %v", err)
	} // Should not return an error

	// Try deleting from a non-existent bucket
	if err := db.Hdel("non_existent_bucket_for_hdel", "some_field"); err != nil {
		t.Fatalf("Hdel from non-existent bucket failed: %v", err)
	} // Should not return an error
}

// TestHmdel tests the Hmdel operation.
func TestHmdel(t *testing.T) {
	db, err := Open("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	key := "batch_delete_test"
	fields := []string{"field1", "field2", "field3", "field4"}
	values := map[string][]byte{
		"field1": []byte("val1"),
		"field2": []byte("val2"),
		"field3": []byte("val3"),
		"field4": []byte("val4"),
	}

	// Set multiple fields
	if err := db.Hmset(key, values); err != nil {
		t.Fatalf("Hmset failed: %v", err)
	}

	fieldsToDelete := []string{"field1", "field3", "non_existent_field"} // Include a non-existent field
	// Batch delete
	if err := db.Hmdel(key, fieldsToDelete); err != nil {
		t.Fatalf("Hmdel failed: %v", err)
	}

	// Check if correct fields are deleted and others remain
	expectedRemaining := map[string]bool{
		"field2": true,
		"field4": true,
	}

	for _, field := range fields {
		exists, err := db.HhasKey(key, field)
		if err != nil {
			t.Fatalf("HhasKey failed: %v", err)
		}

		if expectedRemaining[field] {
			if !exists {
				t.Errorf("field %q should exist but was deleted", field)
			}
		} else {
			if exists {
				t.Errorf("field %q should be deleted but still exists", field)
			}
		}
	}
}

// TestHscan tests the Hscan operation with byte slices.
func TestHscan(t *testing.T) {
	db, err := Open("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	key := "scan_test"
	data := map[string][]byte{ // Values are now []byte
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	// Set data
	if err := db.Hmset(key, data); err != nil {
		t.Fatalf("Hmset failed: %v", err)
	}

	// Scan data
	result, err := db.Hscan(key)
	if err != nil {
		t.Fatalf("Hscan failed: %v", err)
	}

	// Use the new helper for map comparison
	if !equalByteMap(result, data) {
		t.Errorf("Hscan result mismatch: expected %v, got %v", data, result)
	}

	// Test scanning a non-existent key
	nonExistentScan, err := db.Hscan("non_existent_scan_key")
	if err != nil {
		t.Fatalf("Hscan for non-existent key failed: %v", err)
	}
	if len(nonExistentScan) != 0 {
		t.Errorf("expected empty map for non-existent key, got %v", nonExistentScan)
	}
}

// TestHprefix tests the Hprefix operation with byte slices.
func TestHprefix(t *testing.T) {
	db, err := Open("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	key := "prefix_test"
	data := map[string][]byte{ // Values are now []byte
		"user:1":  []byte("Alice"),
		"user:2":  []byte("Bob"),
		"post:1":  []byte("First post"),
		"admin:1": []byte("Admin User"),
	}

	// Set data
	if err := db.Hmset(key, data); err != nil {
		t.Fatalf("Hmset failed: %v", err)
	}

	tests := []struct {
		prefix   string
		expected map[string][]byte
	}{
		{
			"user:",
			map[string][]byte{
				"user:1": []byte("Alice"),
				"user:2": []byte("Bob"),
			},
		},
		{
			"post:",
			map[string][]byte{
				"post:1": []byte("First post"),
			},
		},
		{
			"admin:",
			map[string][]byte{
				"admin:1": []byte("Admin User"),
			},
		},
		{
			"nonexistent:",
			map[string][]byte{}, // Expected empty for no matches
		},
		{
			"", // Empty prefix should return all
			map[string][]byte{
				"user:1":  []byte("Alice"),
				"user:2":  []byte("Bob"),
				"post:1":  []byte("First post"),
				"admin:1": []byte("Admin User"),
			},
		},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("Prefix_%q", tc.prefix), func(t *testing.T) {
			result, err := db.Hprefix(key, tc.prefix)
			if err != nil {
				t.Fatalf("Hprefix failed: %v", err)
			}

			if !equalByteMap(result, tc.expected) { // Use equalByteMap
				t.Errorf("Hprefix result mismatch for prefix %q: expected %v, got %v", tc.prefix, tc.expected, result)
			}
		})
	}

	// Test scanning a non-existent key
	nonExistentPrefixScan, err := db.Hprefix("non_existent_prefix_key", "any:")
	if err != nil {
		t.Fatalf("Hprefix for non-existent key failed: %v", err)
	}
	if len(nonExistentPrefixScan) != 0 {
		t.Errorf("expected empty map for non-existent key, got %v", nonExistentPrefixScan)
	}
}

// TestHrscan tests the Hrscan operation with byte slices.
func TestHrscan(t *testing.T) {
	db, err := Open("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	key := "reverse_scan_test"
	data := map[string][]byte{ // Values are now []byte
		"a": []byte("value1"),
		"b": []byte("value2"),
		"c": []byte("value3"),
		"d": []byte("value4"),
	}

	// Set data
	if err := db.Hmset(key, data); err != nil {
		t.Fatalf("Hmset failed: %v", err)
	}

	// Reverse scan
	result, err := db.Hrscan(key)
	if err != nil {
		t.Fatalf("Hrscan failed: %v", err)
	}

	// Maps do not guarantee iteration order, so we compare content using equalByteMap.
	if !equalByteMap(result, data) {
		t.Errorf("Hrscan result mismatch: expected %v, got %v", data, result)
	}

	// Test scanning a non-existent key
	nonExistentHrscan, err := db.Hrscan("non_existent_hrscan_key")
	if err != nil {
		t.Fatalf("Hrscan for non-existent key failed: %v", err)
	}
	if len(nonExistentHrscan) != 0 {
		t.Errorf("expected empty map for non-existent key, got %v", nonExistentHrscan)
	}
}

// TestHdelBucket tests deleting an entire hash and its associated sorted set index (if any).
func TestHdelBucket(t *testing.T) {
	db, err := Open("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	key := "delete_bucket_test"
	field := "test_field"
	zsetKey := "delete_bucket_zset_test"
	zsetMember := "zmember"
	zsetScore := 10.0

	// Set value in a hash
	if err := db.Hset(key, field, []byte("value")); err != nil {
		t.Fatalf("Hset failed: %v", err)
	}

	// Add member to a sorted set
	if err := db.Zadd(zsetKey, zsetScore, zsetMember); err != nil {
		t.Fatalf("Zadd failed: %v", err)
	}

	// Delete hash bucket
	if err := db.HdelBucket(key); err != nil {
		t.Fatalf("HdelBucket failed: %v", err)
	}

	// Delete sorted set bucket (implicitly also deletes its secondary index)
	if err := db.HdelBucket(zsetKey); err != nil {
		t.Fatalf("HdelBucket for zset failed: %v", err)
	}

	// Verify hash field is gone
	result, err := db.Hget(key, field)
	if err != nil {
		t.Fatalf("Hget failed after HdelBucket: %v", err)
	}
	if result != nil { // Hget now returns nil for non-existent values/keys
		t.Errorf("value still retrievable after bucket deletion: %q", result)
	}

	// Verify sorted set member is gone
	score, err := db.Zscore(zsetKey, zsetMember)
	if err != nil {
		t.Fatalf("Zscore failed after HdelBucket (zset): %v", err)
	}
	if score != 0 {
		t.Errorf("sorted set member still retrievable after bucket deletion, score: %f", score)
	}

	// Verify zcard is 0
	card, err := db.Zcard(zsetKey)
	if err != nil {
		t.Fatalf("Zcard failed after HdelBucket (zset): %v", err)
	}
	if card != 0 {
		t.Errorf("Zcard should be 0 after bucket deletion, got %d", card)
	}
}

// TestZaddZrange tests Zadd and Zrange, including negative indexing and empty ranges.
func TestZaddZrange(t *testing.T) {
	db, err := Open("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	key := "zset_test"

	// Add members
	members := []struct {
		score  float64
		member string
	}{
		{10, "member1"},
		{20, "member2"},
		{30, "member3"},
		{40, "member4"},
		{50, "member5"},
	}

	for _, m := range members {
		if err := db.Zadd(key, m.score, m.member); err != nil {
			t.Fatalf("Zadd failed for %s: %v", m.member, err)
		}
	}

	// Test updating a member's score and re-ordering
	if err := db.Zadd(key, 5, "member3"); err != nil { // member3 now has score 5
		t.Fatalf("Zadd update failed: %v", err)
	}
	if err := db.Zadd(key, 60, "member1"); err != nil { // member1 now has score 60
		t.Fatalf("Zadd update failed: %v", err)
	}

	// Expected sorted order after changes: member3(5), member2(20), member4(40), member5(50), member1(60)
	expectedOrderAfterChanges := []string{"member3", "member2", "member4", "member5", "member1"}
	currentRange, err := db.Zrange(key, 0, -1)
	if err != nil {
		t.Fatalf("Zrange after re-order failed: %v", err)
	}
	if !equal(currentRange, expectedOrderAfterChanges) {
		t.Errorf("Zrange re-order mismatch: expected %v, got %v", expectedOrderAfterChanges, currentRange)
	}

	tests := []struct {
		start    int
		stop     int
		expected []string
	}{
		{0, -1, []string{"member3", "member2", "member4", "member5", "member1"}}, // All members after re-order
		{0, 2, []string{"member3", "member2", "member4"}},
		{-3, -1, []string{"member4", "member5", "member1"}}, // -3 from end is member4
		{-5, -3, []string{"member3", "member2", "member4"}}, // -5 from end is member3
		{2, -2, []string{"member4", "member5"}},
		{0, 10, []string{"member3", "member2", "member4", "member5", "member1"}},   // Out of range, should return all valid
		{-10, 10, []string{"member3", "member2", "member4", "member5", "member1"}}, // Large range, should return all valid
		{3, 1, []string{}},    // Invalid range (start > stop)
		{10, 12, []string{}},  // Out of bounds, empty result
		{-10, -8, []string{}}, // Corrected: This range is entirely out of bounds for 5 elements. Expected empty.
	}

	for _, test := range tests {
		testName := fmt.Sprintf("start=%d,stop=%d", test.start, test.stop)
		t.Run(testName, func(t *testing.T) {
			members, err := db.Zrange(key, test.start, test.stop)
			if err != nil {
				t.Fatalf("Zrange failed: %v", err)
			}

			if !equal(members, test.expected) {
				t.Errorf("member order mismatch: expected %v, got %v", test.expected, members)
			}
		})
	}
}

// TestZrevrange tests Zrevrange, including negative indexing and empty ranges.
func TestZrevrange(t *testing.T) {
	db, err := Open("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	key := "zset_rev_test"

	// Add members
	members := []struct {
		score  float64
		member string
	}{
		{10, "member1"},
		{20, "member2"},
		{30, "member3"},
		{40, "member4"},
		{50, "member5"},
	}

	for _, m := range members {
		if err := db.Zadd(key, m.score, m.member); err != nil {
			t.Fatalf("Zadd failed: %v", err)
		}
	}

	// Test updating a member's score to change reverse order
	if err := db.Zadd(key, 5, "member4"); err != nil { // member4 now has score 5
		t.Fatalf("Zadd update failed: %v", err)
	}
	if err := db.Zadd(key, 60, "member2"); err != nil { // member2 now has score 60
		t.Fatalf("Zadd update failed: %v", err)
	}

	// Expected sorted order: member4(5), member1(10), member3(30), member5(50), member2(60)
	// Expected reverse order: member2, member5, member3, member1, member4

	tests := []struct {
		start    int
		stop     int
		expected []string
	}{
		{0, -1, []string{"member2", "member5", "member3", "member1", "member4"}}, // All members after re-order
		{0, 2, []string{"member2", "member5", "member3"}},
		{-3, -1, []string{"member3", "member1", "member4"}}, // -3 from end is member3
		{-5, -3, []string{"member2", "member5", "member3"}}, // -5 from end is member2
		{2, -2, []string{"member3", "member1"}},
		{0, 10, []string{"member2", "member5", "member3", "member1", "member4"}}, // Out of range
		{-10, 10, []string{"member2", "member5", "member3", "member1", "member4"}},
		{3, 1, []string{}},   // Invalid range
		{10, 12, []string{}}, // Out of bounds, empty result
	}

	for _, test := range tests {
		testName := fmt.Sprintf("start=%d,stop=%d", test.start, test.stop)
		t.Run(testName, func(t *testing.T) {
			members, err := db.Zrevrange(key, test.start, test.stop)
			if err != nil {
				t.Fatalf("Zrevrange failed: %v", err)
			}

			if !equal(members, test.expected) {
				t.Errorf("member order mismatch: expected %v, got %v", test.expected, members)
			}
		})
	}
}

// TestZscore tests Zscore with the optimized secondary index lookup.
func TestZscore(t *testing.T) {
	db, err := Open("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	key := "zset_score_test"
	member := "test_member"
	score := 99.5

	// Add member
	if err := db.Zadd(key, score, member); err != nil {
		t.Fatalf("Zadd failed: %v", err)
	}

	// Get score
	result, err := db.Zscore(key, member)
	if err != nil {
		t.Fatalf("Zscore failed: %v", err)
	}
	if result != score {
		t.Errorf("score mismatch: expected %f, got %f", score, result)
	}

	// Test score update
	newScore := 123.45
	if err := db.Zadd(key, newScore, member); err != nil {
		t.Fatalf("Zadd update failed: %v", err)
	}
	updatedResult, err := db.Zscore(key, member)
	if err != nil {
		t.Fatalf("Zscore after update failed: %v", err)
	}
	if updatedResult != newScore {
		t.Errorf("score update mismatch: expected %f, got %f", newScore, updatedResult)
	}

	// Test non-existent member
	nonExistentScore, err := db.Zscore(key, "non_existent_member")
	if err != nil {
		t.Fatalf("Zscore for non-existent member failed: %v", err)
	}
	if nonExistentScore != 0 {
		t.Errorf("expected 0 for non-existent member score, got %f", nonExistentScore)
	}

	// Test non-existent sorted set key
	nonExistentKeyScore, err := db.Zscore("non_existent_zset_key", "any_member")
	if err != nil {
		t.Fatalf("Zscore for non-existent zset key failed: %v", err)
	}
	if nonExistentKeyScore != 0 {
		t.Errorf("expected 0 for non-existent zset key, got %f", nonExistentKeyScore)
	}
}

// TestZrem tests Zrem with the optimized secondary index lookup.
func TestZrem(t *testing.T) {
	db, err := Open("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	key := "zset_rem_test"
	member1 := "to_remove1"
	member2 := "to_remove2"
	member3 := "keep_this_one"

	// Add members
	if err := db.Zadd(key, 100, member1); err != nil {
		t.Fatalf("Zadd failed: %v", err)
	}
	if err := db.Zadd(key, 110, member2); err != nil {
		t.Fatalf("Zadd failed: %v", err)
	}
	if err := db.Zadd(key, 90, member3); err != nil {
		t.Fatalf("Zadd failed: %v", err)
	}

	// Get initial card
	initialCard, err := db.Zcard(key)
	if err != nil {
		t.Fatalf("Zcard failed: %v", err)
	}
	if initialCard != 3 {
		t.Errorf("initial Zcard mismatch: expected 3, got %d", initialCard)
	}

	// Remove member1
	if err := db.Zrem(key, member1); err != nil {
		t.Fatalf("Zrem failed: %v", err)
	}

	// Check if member1's score is 0 and Zcard decreased
	score1, err := db.Zscore(key, member1)
	if err != nil {
		t.Fatalf("Zscore after Zrem failed: %v", err)
	}
	if score1 != 0 {
		t.Errorf("member1 not removed correctly, score should be 0, got %f", score1)
	}

	cardAfterOneRem, err := db.Zcard(key)
	if err != nil {
		t.Fatalf("Zcard failed: %v", err)
	}
	if cardAfterOneRem != 2 {
		t.Errorf("Zcard mismatch after one removal: expected 2, got %d", cardAfterOneRem)
	}

	// Remove a non-existent member (should not error)
	if err := db.Zrem(key, "non_existent_member"); err != nil {
		t.Fatalf("Zrem for non-existent member failed: %v", err)
	}

	// Check that member3 is still present
	score3, err := db.Zscore(key, member3)
	if err != nil {
		t.Fatalf("Zscore for member3 failed: %v", err)
	}
	if score3 != 90 {
		t.Errorf("member3's score changed unexpectedly: expected 90, got %f", score3)
	}
}

// TestZcard tests Zcard, including empty sets.
func TestZcard(t *testing.T) {
	db, err := Open("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	key := "zset_card_test"
	emptyKey := "empty_zset_key"

	// Initial count for empty key
	countEmpty, err := db.Zcard(emptyKey)
	if err != nil {
		t.Fatalf("Zcard for empty key failed: %v", err)
	}
	if countEmpty != 0 {
		t.Errorf("expected 0 for empty zset, got %d", countEmpty)
	}

	// Add members
	members := []struct {
		score  float64
		member string
	}{
		{10, "member1"},
		{20, "member2"},
		{30, "member3"},
	}

	for _, m := range members {
		if err := db.Zadd(key, m.score, m.member); err != nil {
			t.Fatalf("Zadd failed: %v", err)
		}
	}

	// Get member count
	count, err := db.Zcard(key)
	if err != nil {
		t.Fatalf("Zcard failed: %v", err)
	}
	if count != len(members) {
		t.Errorf("member count mismatch: expected %d, got %d", len(members), count)
	}

	// Add another member and check count
	if err := db.Zadd(key, 40, "member4"); err != nil {
		t.Fatalf("Zadd failed: %v", err)
	}
	count, err = db.Zcard(key)
	if err != nil {
		t.Fatalf("Zcard failed: %v", err)
	}
	if count != 4 {
		t.Errorf("member count mismatch after adding: expected 4, got %d", count)
	}

	// Remove a member and check count
	if err := db.Zrem(key, "member4"); err != nil {
		t.Fatalf("Zrem failed: %v", err)
	}
	count, err = db.Zcard(key)
	if err != nil {
		t.Fatalf("Zcard failed: %v", err)
	}
	if count != 3 {
		t.Errorf("member count mismatch after removing: expected 3, got %d", count)
	}
}

// Helper function: checks if two string slices are equal (used for Zrange/Zrevrange)
func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// Helper function: checks if two map[string][]byte are deeply equal (used for Hscan/Hprefix/Hrscan)
func equalByteMap(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v1 := range a {
		v2, ok := b[k]
		if !ok || !bytes.Equal(v1, v2) {
			return false
		}
	}
	return true
}
