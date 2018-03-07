package redis

import (
	"fmt"

	lib "github.com/garyburd/redigo/redis"
)

// Removes the specified fields from the hash stored at key.
//
// Integer reply v: the number of fields that were removed from the hash, not
// including specified but non existing fields.
//
// Time complexity: O(N) where N is the number of fields to be removed.
func (cp *ConnPool) HDEL(key string, fields ...interface{}) (v int64, err error) {
	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	args = append(args, fields...)
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("HDEL", args...))
	conn.Close()
	return
}

// Returns if field is an existing field in the hash stored at key.
//
// Boolean reply v, specifically:
// true if the hash contains field.
// false if the hash does not contain field, or key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) HEXISTS(key string, field interface{}) (v bool, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Bool(conn.Do("HEXISTS", key, field))
	conn.Close()
	return
}

// Returns the value associated with field in the hash stored at key.
//
// Bulk string reply v: the value associated with field.
// ErrNil when field is not present in the hash or key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) HGET(key string, field interface{}) (v string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.String(conn.Do("HGET", key, field))
	conn.Close()
	if err == lib.ErrNil {
		err = ErrNil
	}
	return
}

// Returns all fields and values of the hash stored at key.
//
// Map reply v: map of fields and their values stored in the hash, or an empty map when key does not exist.
//
// Time complexity: O(N) where N is the size of the hash.
func (cp *ConnPool) HGETALL(key string) (v map[string]string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.StringMap(conn.Do("HGETALL", key))
	conn.Close()
	return
}

// Increments the number stored at field in the hash stored at key by increment.
//
// Integer reply v: the value at field after the increment operation.
//
// Time complexity: O(1)
func (cp *ConnPool) HINCRBY(key string, field, increment interface{}) (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("HINCRBY", key, field, increment))
	conn.Close()
	return
}

// Increment the specified field of a hash stored at key, and representing a
// floating point number, by the specified increment.
//
// Float reply v: the value of field after the increment.
//
// Time complexity: O(1)
func (cp *ConnPool) HINCRBYFLOAT(key string, field, increment interface{}) (v float64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Float64(conn.Do("HINCRBYFLOAT", key, field, increment))
	conn.Close()
	return
}

// Time complexity: O(N) where N is the size of the hash.
//
// Array reply v: list of fields in the hash, or an empty list when key does not exist.
//
// Time complexity: O(N) where N is the size of the hash.
func (cp *ConnPool) HKEYS(key string) (v []string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Strings(conn.Do("HKEYS", key))
	conn.Close()
	return
}

// Returns the number of fields contained in the hash stored at key.
//
// Integer reply v: number of fields in the hash, or 0 when key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) HLEN(key string) (v int64, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Int64(conn.Do("HLEN", key))
	conn.Close()
	return
}

// Returns the values associated with the specified fields in the hash stored at key.
//
// Map reply v: map of given fields and their values.
//
// Time complexity: O(N) where N is the number of fields being requested.
func (cp *ConnPool) HMGET(key string, fields ...interface{}) (map[string]string, error) {
	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	args = append(args, fields...)
	conn := cp.GetSlaveConn()
	t, err := lib.Strings(conn.Do("HMGET", args...))
	conn.Close()
	if err != nil {
		return nil, err
	}
	if len(t) != len(fields) {
		return nil, lib.ErrNil
	}
	m := make(map[string]string, len(fields))
	for i, f := range fields {
		m[fmt.Sprintf("%v", f)] = t[i]
	}
	return m, nil
}

// Sets the specified fields to their respective values in the hash stored at key.
//
// Time complexity: O(N) where N is the number of fields being set.
func (cp *ConnPool) HMSET(key string, m map[string]interface{}) (err error) {
	args := make([]interface{}, 0, len(m)*2+1)
	args = append(args, key)
	for k, v := range m {
		args = append(args, k, v)
	}
	conn := cp.GetMasterConn()
	_, err = conn.Do("HMSET", args...)
	conn.Close()
	return
}

func (cp *ConnPool) HSCAN() {
	conn := cp.GetSlaveConn()
	// @todo
	conn.Close()
	return
}

// Sets field in the hash stored at key to value.
//
// Integer reply v, specifically:
// 1 if field is a new field in the hash and value was set.
// 0 if field already exists in the hash and the value was updated.
//
// Time complexity: O(1)
func (cp *ConnPool) HSET(key string, field, value interface{}) (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("HSET", key, field, value))
	conn.Close()
	return
}

// Sets field in the hash stored at key to value, only if field does not yet exist.
//
// Boolean reply v, specifically:
// true if field is a new field in the hash and value was set.
// false if field already exists in the hash and no operation was performed.
//
// Time complexity: O(1)
func (cp *ConnPool) HSETNX(key string, field, value interface{}) (v bool, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Bool(conn.Do("HSETNX", key, field, value))
	conn.Close()
	return
}

// Available since 3.2.0.
//
// Returns the string length of the value associated with field in the hash stored at key.
//
// Integer reply v: the string length of the value associated with field, or zero
// when field is not present in the hash or key does not exist at all.
//
// Time complexity: O(1)
func (cp *ConnPool) HSTRLEN(key string, field interface{}) (v int64, err error) {
	if cp.lessThan("3.2.0") {
		return 0, ErrNotSupport
	}
	conn := cp.GetSlaveConn()
	v, err = lib.Int64(conn.Do("HSTRLEN", key, field))
	conn.Close()
	return
}

// Returns all values in the hash stored at key.
//
// Array reply v: list of values in the hash, or an empty list when key does not exist.
//
// Time complexity: O(N) where N is the size of the hash.
func (cp *ConnPool) HVALS(key string) (v []string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Strings(conn.Do("HVALS", key))
	conn.Close()
	return
}
