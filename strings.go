package redis

import (
	"fmt"

	lib "github.com/garyburd/redigo/redis"
)

type BitOperate string

const (
	BitAnd BitOperate = "AND"
	BitOr  BitOperate = "OR"
	BitXor BitOperate = "XOR"
	BitNot BitOperate = "NOT"
)

// If key already exists and is a string, this command appends the value at
// the end of the string. If key does not exist it is created and set as an empty
// string, so APPEND will be similar to SET in this special case.
//
// Integer reply v: the length of the string after the append operation.
//
// Time complexity: O(1). The amortized time complexity is O(1) assuming the
// appended value is small and the already present value is of any size, since the
// dynamic string library used by Redis will double the free space available on every reallocation.
func (cp *ConnPool) APPEND(key, value string) (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("APPEND", key, value))
	conn.Close()
	return
}

// Count the number of set bits (population counting) in a string.
//
// Integer reply v: The number of bits set to 1.
//
// Time complexity: O(N)
func (cp *ConnPool) BITCOUNT(key string, start, end interface{}) (v int64, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Int64(conn.Do("BITCOUNT", key, start, end))
	conn.Close()
	return
}

// Available since 2.8.7.
//
// Time complexity: O(1) for each subcommand specified
func (cp *ConnPool) BITFIELD() error {
	if cp.lessThan("2.8.7") {
		return ErrNotSupport
	}
	// @todo
	return nil
}

// Perform a bitwise operation between multiple keys (containing string
// values) and store the result in the destination key.
//
// Integer reply v: The size of the string stored in the destination key, that is equal to
// the size of the longest input string.
//
// Time complexity: O(N)
func (cp *ConnPool) BITOP(operate BitOperate, destKey string, keys ...interface{}) (v int64, err error) {
	args := make([]interface{}, 0, len(keys)+2)
	args = append(args, operate, destKey)
	args = append(args, keys...)
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("BITOP", args...))
	conn.Close()
	return
}

// Available since 2.8.7.
//
// Return the position of the first bit set to 1 or 0 in a string.
//
// Integer reply v: The command returns the position of the first bit set to 1 or 0 according to the request.
// -1 if we look for set bits (the bit argument is 1) and the string is empty or composed of just zero bytes.
//
// Time complexity: O(N)
func (cp *ConnPool) BITPOS(key string, bit, start, end interface{}) (v int64, err error) {
	if cp.lessThan("2.8.7") {
		return 0, ErrNotSupport
	}
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("BITPOS", key, bit, start, end))
	conn.Close()
	return
}

// Decrements the number stored at key by one.
//
// Integer reply v: the value of key after the decrement.
//
// Time complexity: O(1)
func (cp *ConnPool) DECR(key string) (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("DECR", key))
	conn.Close()
	return
}

// Decrements the number stored at key by decrement.
//
// Integer reply v: the value of key after the decrement.
//
// Time complexity: O(1)
func (cp *ConnPool) DECRBY(key string, decrement interface{}) (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("DECRBY", key, decrement))
	conn.Close()
	return
}

// Get the value of key.
//
// Bulk string reply v: the value of key.
// ErrNil when key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) GET(key string) (v string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.String(conn.Do("GET", key))
	conn.Close()
	if err == lib.ErrNil {
		err = ErrNil
	}
	return
}

// Returns the bit value at offset in the string value stored at key.
//
// Integer reply v: the bit value stored at offset.
//
// Time complexity: O(1)
func (cp *ConnPool) GETBIT(key string, offset interface{}) (v int64, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Int64(conn.Do("GETBIT", key, offset))
	conn.Close()
	return
}

// Returns the substring of the string value stored at key, determined by the
// offsets start and end (both are inclusive).
//
// Bulk string reply v.
//
// Time complexity: O(N) where N is the length of the returned string. The
// complexity is ultimately determined by the returned length, but because
// creating a substring from an existing string is very cheap, it can be considered
// O(1) for small strings.
func (cp *ConnPool) GETRANGE(key string, start, end interface{}) (v string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.String(conn.Do("GETRANGE", key, start, end))
	conn.Close()
	return
}

// Atomically sets key to value and returns the old value stored at key.
//
// Bulk string reply: the old value stored at key.
// ErrNil when key did not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) GETSET(key string, value interface{}) (v string, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.String(conn.Do("GETSET", key, value))
	conn.Close()
	if err == lib.ErrNil {
		err = ErrNil
	}
	return
}

// Increments the number stored at key by one.
//
// Integer reply v: the value of key after the increment
//
// Time complexity: O(1)
func (cp *ConnPool) INCR(key string) (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("INCR", key))
	conn.Close()
	return
}

// Increments the number stored at key by increment.
//
// Integer reply v: the value of key after the increment
//
// Time complexity: O(1)
func (cp *ConnPool) INCRBY(key string, increment interface{}) (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("INCRBY", key, increment))
	conn.Close()
	return
}

// Increment the string representing a floating point number stored at key by
// the specified increment.
//
// Float reply v: the value of key after the increment.
//
// Time complexity: O(1)
func (cp *ConnPool) INCRBYFLOAT(key string, increment interface{}) (v float64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Float64(conn.Do("INCRBYFLOAT", key, increment))
	conn.Close()
	return
}

// Returns the values of all specified keys.
//
// Map reply v: map of keys and values.
//
// Time complexity: O(N) where N is the number of keys to retrieve.
func (cp *ConnPool) MGET(keys ...interface{}) (map[string]string, error) {
	conn := cp.GetSlaveConn()
	t, err := lib.Strings(conn.Do("MGET", keys...))
	conn.Close()
	if err != nil {
		return nil, err
	}
	if len(keys) != len(t) {
		return nil, lib.ErrNil
	}
	m := make(map[string]string, len(keys))
	for i, v := range keys {
		k := fmt.Sprintf("%v", v)
		m[k] = t[i]
	}
	return m, nil
}

// Sets the given keys to their respective values.
//
// Time complexity: O(N) where N is the number of keys to set.
func (cp *ConnPool) MSET(key string, m map[string]interface{}) (err error) {
	args := make([]interface{}, 0, len(m)+1)
	args = append(args, key)
	for k, v := range m {
		args = append(args, k, v)
	}
	conn := cp.GetMasterConn()
	_, err = conn.Do("MSET", args...)
	conn.Close()
	return
}

// Sets the given keys to their respective values. MSETNX will not perform any
// operation at all even if just a single key already exists.
//
// Boolean reply v, specifically:
// true if the all the keys were set.
// false if no key was set (at least one key already existed).
//
// Time complexity: O(N) where N is the number of keys to set.
func (cp *ConnPool) MSETNX(key string, m map[string]interface{}) (v bool, err error) {
	args := make([]interface{}, 0, len(m)+1)
	args = append(args, key)
	for k, v := range m {
		args = append(args, k, v)
	}
	conn := cp.GetMasterConn()
	v, err = lib.Bool(conn.Do("MSETNX", args...))
	conn.Close()
	return
}

// PSETEX works exactly like SETEX with the sole difference that the expire
// time is specified in milliseconds instead of seconds.
//
// Time complexity: O(1)
func (cp *ConnPool) PSETEX(key string, milliseconds, value interface{}) (err error) {
	conn := cp.GetMasterConn()
	_, err = conn.Do("PSETEX", key, milliseconds, value)
	conn.Close()
	return
}

// Set key to hold the string value. If key already holds a value, it is
// overwritten, regardless of its type.
//
// Time complexity: O(1)
func (cp *ConnPool) SET(key string, value interface{}) (err error) {
	return cp.SETExtra(key, value, 0, 0, false, false)
}

// > 2.6.12
// EX seconds -- Set the specified expire time, in seconds.
// PX milliseconds -- Set the specified expire time, in milliseconds.
// NX -- Only set the key if it does not already exist.
// XX -- Only set the key if it already exist.
//
// If seconds is set, milliseconds will be ignored.
func (cp *ConnPool) SETExtra(key string, value interface{}, seconds, milliseconds int64, nx, xx bool) (err error) {
	args := make([]interface{}, 0, 6)
	args = append(args, key, value)
	if seconds > 0 {
		args = append(args, "EX", seconds)
	} else if milliseconds > 0 {
		args = append(args, "PX", milliseconds)
	}
	if nx {
		args = append(args, "NX")
	}
	if xx {
		args = append(args, "XX")
	}
	conn := cp.GetMasterConn()
	_, err = conn.Do("SET", args...)
	conn.Close()
	return
}

// Sets or clears the bit at offset in the string value stored at key.
//
// Integer reply v: the original bit value stored at offset.
//
// Time complexity: O(1)
func (cp *ConnPool) SETBIT(key string, offset, value interface{}) (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("SETBIT", key, offset, value))
	conn.Close()
	return
}

// Set key to hold the string value and set key to timeout after a given number of seconds.
//
// Time complexity: O(1)
func (cp *ConnPool) SETEX(key string, seconds, value interface{}) (err error) {
	conn := cp.GetMasterConn()
	_, err = conn.Do("SETEX", key, seconds, value)
	conn.Close()
	return
}

// Set key to hold string value if key does not exist.
//
// Boolean reply v, specifically:
// true if the key was set
// false if the key was not set
//
// Time complexity: O(1)
func (cp *ConnPool) SETNX(key string, value interface{}) (v bool, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Bool(conn.Do("SETNX", key, value))
	conn.Close()
	return
}

// Overwrites part of the string stored at key, starting at the specified offset,
// for the entire length of value.
//
// Integer reply v: the length of the string after it was modified by the command.
//
// Time complexity: O(1), not counting the time taken to copy the new string in
// place. Usually, this string is very small so the amortized complexity is O(1).
// Otherwise, complexity is O(M) with M being the length of the value argument.
func (cp *ConnPool) SETRANGE(key string, offset interface{}, value string) (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("SETRANGE", key, offset, value))
	conn.Close()
	return
}

// Returns the length of the string value stored at key.
//
// Integer reply v: the length of the string at key, or 0 when key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) STRLEN(key string) (v int64, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Int64(conn.Do("STRLEN", key))
	conn.Close()
	return
}
