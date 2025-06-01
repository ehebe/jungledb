package jungledb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.etcd.io/bbolt"
)

// DB represents the database instance.
type DB struct {
	db       *bbolt.DB
	filePath string
	mu       sync.RWMutex
}

// Open opens or creates a JungleDB database file.
func Open(filePath string) (*DB, error) {
	if err := ensureDir(filePath); err != nil {
		return nil, err
	}

	db, err := bbolt.Open(filePath, 0666, &bbolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	return &DB{
		db:       db,
		filePath: filePath,
	}, nil
}

// Close closes the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.db.Close()
}

// Hset sets the field value in a hash.
// Accepts []byte for value to minimize conversions.
func (db *DB) Hset(key, field string, value []byte) error {
	return db.update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(key))
		if err != nil {
			return fmt.Errorf("failed to create bucket: %v", err)
		}
		return bucket.Put([]byte(field), value)
	})
}

// Hget retrieves the value of a field in a hash.
// Returns []byte to minimize conversions.
func (db *DB) Hget(key, field string) ([]byte, error) {
	var value []byte
	err := db.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(key))
		if bucket == nil {
			return nil // Bucket does not exist, return nil
		}
		value = bucket.Get([]byte(field))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

// Hmset sets multiple field values in a hash.
func (db *DB) Hmset(key string, fields map[string][]byte) error {
	return db.update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(key))
		if err != nil {
			return fmt.Errorf("failed to create bucket: %v", err)
		}

		for field, value := range fields {
			if err := bucket.Put([]byte(field), value); err != nil {
				return err
			}
		}
		return nil
	})
}

// Hmget retrieves the values of multiple fields in a hash.
func (db *DB) Hmget(key string, fields []string) ([][]byte, error) {
	values := make([][]byte, len(fields))

	err := db.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(key))
		if bucket == nil {
			return nil // Bucket does not exist, return slice of nils
		}

		for i, field := range fields {
			values[i] = bucket.Get([]byte(field))
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return values, nil
}

// Hincr increments the integer value of a field in a hash.
// Values are stored and retrieved as 8-byte binary integers.
func (db *DB) Hincr(key, field string, delta int64) (int64, error) {
	var newValue int64
	err := db.update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(key))
		if err != nil {
			return fmt.Errorf("failed to create bucket: %v", err)
		}

		currentValueBytes := bucket.Get([]byte(field))
		currentValue := int64(0)

		if currentValueBytes != nil {
			if len(currentValueBytes) != 8 {
				return errors.New("field value is not a valid 8-byte integer")
			}
			currentValue = int64(binary.BigEndian.Uint64(currentValueBytes))
		}

		newValue = currentValue + delta

		// Check for overflow
		if (delta > 0 && newValue < currentValue) || (delta < 0 && newValue > currentValue) {
			return errors.New("integer overflow")
		}

		// Save new value as 8-byte binary
		newValueBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(newValueBytes, uint64(newValue))
		return bucket.Put([]byte(field), newValueBytes)
	})

	if err != nil {
		return 0, err
	}

	return newValue, nil
}

// HgetInt retrieves the integer value of a field in a hash.
// Values are retrieved as 8-byte binary integers.
func (db *DB) HgetInt(key, field string) (int64, error) {
	var value int64
	err := db.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(key))
		if bucket == nil {
			return nil // Bucket does not exist, return 0
		}

		valueBytes := bucket.Get([]byte(field))
		if valueBytes == nil {
			return nil // Field does not exist, return 0
		}

		if len(valueBytes) != 8 {
			return errors.New("field value is not a valid 8-byte integer")
		}
		value = int64(binary.BigEndian.Uint64(valueBytes))
		return nil
	})

	if err != nil {
		return 0, err
	}

	return value, nil
}

// HhasKey checks if a field exists in a hash.
func (db *DB) HhasKey(key, field string) (bool, error) {
	var exists bool
	err := db.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(key))
		if bucket == nil {
			return nil // Bucket does not exist, return false
		}

		exists = bucket.Get([]byte(field)) != nil
		return nil
	})

	if err != nil {
		return false, err
	}

	return exists, nil
}

// Hdel deletes a field from a hash.
func (db *DB) Hdel(key, field string) error {
	return db.update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(key))
		if bucket == nil {
			return nil // Bucket does not exist, nothing to delete
		}

		return bucket.Delete([]byte(field))
	})
}

// Hmdel deletes multiple fields from a hash.
func (db *DB) Hmdel(key string, fields []string) error {
	return db.update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(key))
		if bucket == nil {
			return nil // Bucket does not exist, nothing to delete
		}

		for _, field := range fields {
			if err := bucket.Delete([]byte(field)); err != nil {
				return err
			}
		}
		return nil
	})
}

// Hscan scans all fields and values in a hash.
// Returns map[string][]byte to minimize conversions.
func (db *DB) Hscan(key string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	err := db.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(key))
		if bucket == nil {
			return nil // Bucket does not exist, return empty map
		}

		return bucket.ForEach(func(k, v []byte) error {
			result[string(k)] = v // Key converted to string for map key, value kept as []byte
			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// Hprefix scans fields in a hash that start with a specified prefix.
// Returns map[string][]byte to minimize conversions.
func (db *DB) Hprefix(key, prefix string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	err := db.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(key))
		if bucket == nil {
			return nil // Bucket does not exist, return empty map
		}

		cursor := bucket.Cursor()
		prefixBytes := []byte(prefix)

		for k, v := cursor.Seek(prefixBytes); k != nil && bytes.HasPrefix(k, prefixBytes); k, v = cursor.Next() {
			result[string(k)] = v // Key converted to string for map key, value kept as []byte
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// Hrscan scans all fields and values in a hash in reverse order.
// Returns map[string][]byte to minimize conversions.
func (db *DB) Hrscan(key string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	err := db.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(key))
		if bucket == nil {
			return nil // Bucket does not exist, return empty map
		}

		cursor := bucket.Cursor()

		// Move to the last key
		for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
			result[string(k)] = v // Key converted to string for map key, value kept as []byte
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// HdelBucket deletes an entire hash.
func (db *DB) HdelBucket(key string) error {
	return db.update(func(tx *bbolt.Tx) error {
		// Also delete the sorted set secondary index if it exists for this key
		// This assumes a convention that sorted set secondary indexes are named key + "_members"
		// If HdelBucket is used for generic bucket deletion, this might need refinement.
		if err := tx.DeleteBucket([]byte(key + "_members")); err != nil && !errors.Is(err, bbolt.ErrBucketNotFound) {
			return fmt.Errorf("failed to delete associated sorted set index bucket: %v", err)
		}
		return tx.DeleteBucket([]byte(key))
	})
}

// Zadd adds a member to a sorted set.
// Implements a secondary index for efficient member lookup.
func (db *DB) Zadd(key string, score float64, member string) error {
	return db.update(func(tx *bbolt.Tx) error {
		// Main sorted set bucket (score-ordered)
		ssBucket, err := tx.CreateBucketIfNotExists([]byte(key))
		if err != nil {
			return fmt.Errorf("failed to create sorted set bucket: %v", err)
		}

		// Secondary index bucket for member lookup (member -> score)
		idxBucket, err := tx.CreateBucketIfNotExists([]byte(key + "_members"))
		if err != nil {
			return fmt.Errorf("failed to create member index bucket: %v", err)
		}

		memberBytes := []byte(member)
		scoreBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(scoreBytes, math.Float64bits(score))

		// Check for existing score for the member and remove the old entry
		existingScoreBytes := idxBucket.Get(memberBytes)
		if existingScoreBytes != nil {
			oldSsKey := append(existingScoreBytes, memberBytes...)
			if err := ssBucket.Delete(oldSsKey); err != nil {
				return fmt.Errorf("failed to delete old sorted set entry for member: %v", err)
			}
		}

		// Store in main sorted set bucket (key: score + member, value: empty)
		ssKey := append(scoreBytes, memberBytes...)
		if err := ssBucket.Put(ssKey, []byte{}); err != nil {
			return fmt.Errorf("failed to put into sorted set bucket: %v", err)
		}

		// Store in secondary index (key: member, value: score)
		return idxBucket.Put(memberBytes, scoreBytes)
	})
}

// Zrange returns members within a specified range in a sorted set (ascending order).
func (db *DB) Zrange(key string, start, stop int) ([]string, error) {
	var members []string
	err := db.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(key))
		if bucket == nil {
			return nil // Bucket does not exist, return empty list
		}

		size := bucket.Stats().KeyN // Get the current size of the bucket for negative index handling

		// Handle negative indices
		if start < 0 {
			start = size + start
			if start < 0 {
				start = 0
			}
		}

		if stop < 0 {
			stop = size + stop
			if stop < 0 {
				stop = -1 // Effectively makes range empty if stop is before start
			}
		}

		if start > stop || start >= size { // Handle empty or out-of-bounds ranges
			return nil
		}

		cursor := bucket.Cursor()
		count := 0

		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			if count >= start {
				// Extract member part (skip the first 8 bytes for score)
				member := string(k[8:])
				members = append(members, member)
			}
			count++

			if count > stop {
				break
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return members, nil
}

// Zrevrange returns members within a specified range in a sorted set (descending order).
func (db *DB) Zrevrange(key string, start, stop int) ([]string, error) {
	var members []string
	err := db.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(key))
		if bucket == nil {
			return nil // Bucket does not exist, return empty list
		}

		size := bucket.Stats().KeyN

		// Handle negative indices
		if start < 0 {
			start = size + start
			if start < 0 {
				start = 0
			}
		}

		if stop < 0 {
			stop = size + stop
			if stop < 0 {
				stop = -1 // Effectively makes range empty if stop is before start
			}
		}

		if start > stop || start >= size { // Handle empty or out-of-bounds ranges
			return nil
		}

		cursor := bucket.Cursor()
		count := 0

		for k, _ := cursor.Last(); k != nil; k, _ = cursor.Prev() {
			if count >= start {
				// Extract member part (skip the first 8 bytes for score)
				member := string(k[8:])
				members = append(members, member)
			}
			count++

			if count > stop {
				break
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return members, nil
}

// Zscore returns the score of a member in a sorted set.
// Uses the secondary index for efficient lookup.
func (db *DB) Zscore(key, member string) (float64, error) {
	var score float64
	err := db.view(func(tx *bbolt.Tx) error {
		idxBucket := tx.Bucket([]byte(key + "_members")) // Use secondary index
		if idxBucket == nil {
			return nil // Index bucket does not exist, so member won't be found
		}

		scoreBytes := idxBucket.Get([]byte(member))
		if scoreBytes == nil {
			return nil // Member not found
		}

		if len(scoreBytes) != 8 {
			return fmt.Errorf("invalid score format for member %s", member)
		}

		score = math.Float64frombits(binary.BigEndian.Uint64(scoreBytes))
		return nil
	})

	if err != nil {
		return 0, err
	}

	return score, nil
}

// Zrem removes a member from a sorted set.
// Uses the secondary index for efficient lookup and deletion.
func (db *DB) Zrem(key, member string) error {
	return db.update(func(tx *bbolt.Tx) error {
		ssBucket := tx.Bucket([]byte(key))
		idxBucket := tx.Bucket([]byte(key + "_members"))

		if ssBucket == nil || idxBucket == nil {
			return nil // Buckets don't exist, nothing to delete
		}

		memberBytes := []byte(member)

		// Get score from secondary index
		scoreBytes := idxBucket.Get(memberBytes)
		if scoreBytes == nil {
			return nil // Member not found in index
		}

		// Delete from main sorted set bucket
		ssKey := append(scoreBytes, memberBytes...)
		if err := ssBucket.Delete(ssKey); err != nil {
			return fmt.Errorf("failed to delete from sorted set bucket: %v", err)
		}

		// Delete from secondary index
		return idxBucket.Delete(memberBytes)
	})
}

// Zcard returns the number of members in a sorted set.
func (db *DB) Zcard(key string) (int, error) {
	var count int
	err := db.view(func(tx *bbolt.Tx) error {
		// Count from the primary sorted set bucket
		bucket := tx.Bucket([]byte(key))
		if bucket == nil {
			return nil // Bucket does not exist, return 0
		}

		count = bucket.Stats().KeyN
		return nil
	})

	if err != nil {
		return 0, err
	}

	return count, nil
}

// Helper function: ensure directory exists.
func ensureDir(filePath string) error {
	dir := filepath.Dir(filePath)
	return os.MkdirAll(dir, 0755) // Create directory with read/write/execute for owner, read/execute for group/others
}

// Helper function: execute read-only transaction.
func (db *DB) view(fn func(tx *bbolt.Tx) error) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.db.View(fn)
}

// Helper function: execute read-write transaction.
func (db *DB) update(fn func(tx *bbolt.Tx) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.db.Update(fn)
}
