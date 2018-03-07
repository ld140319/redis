package redis

import (
	lib "github.com/garyburd/redigo/redis"
)

// Add the specified members to the set stored at key.
//
// Integer reply v: the number of elements that were added to the set, not
// including all the elements already present into the set.
//
// Time complexity: O(1) for each element added, so O(N) to add N elements
// when the command is called with multiple arguments.
func (cp *ConnPool) SADD(key string, members ...interface{}) (v int64, err error) {
	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)
	args = append(args, members...)
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("SADD", args...))
	conn.Close()
	return
}

// Returns the set cardinality (number of elements) of the set stored at key.
//
// Integer reply v: the cardinality (number of elements) of the set, or 0 if
// key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) SCARD(key string) (v int64, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Int64(conn.Do("SCARD", key))
	conn.Close()
	return
}

// Returns the members of the set resulting from the difference between the
// first set and all the successive sets.
//
// Array reply v: list with members of the resulting set.
//
// Time complexity: O(N) where N is the total number of elements in all given sets.
func (cp *ConnPool) SDIFF(keys ...interface{}) (v []string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Strings(conn.Do("SDIFF", keys...))
	conn.Close()
	return
}

// This command is equal to SDIFF, but instead of returning the resulting set,
// it is stored in destination.
//
// Integer reply v: the number of elements in the resulting set.
//
// Time complexity: O(N) where N is the total number of elements in all given sets.
func (cp *ConnPool) SDIFFSTORE(destination string, keys ...interface{}) (v int64, err error) {
	args := make([]interface{}, 0, len(keys)+1)
	args = append(args, destination)
	args = append(args, keys...)
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("SDIFFSTORE", args...))
	conn.Close()
	return
}

// Returns the members of the set resulting from the intersection of all the given sets.
//
// Array reply v: list with members of the resulting set.
//
// Time complexity: O(N*M) worst case where N is the cardinality of the smallest
// set and M is the number of sets.
func (cp *ConnPool) SINTER(keys ...interface{}) (v []string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Strings(conn.Do("SINTER", keys...))
	conn.Close()
	return
}

// This command is equal to SINTER, but instead of returning the resulting set,
// it is stored in destination.
//
// Integer reply v: the number of elements in the resulting set.
//
// Time complexity: O(N*M) worst case where N is the cardinality of the smallest
// set and M is the number of sets.
func (cp *ConnPool) SINTERSTORE(destination string, keys ...interface{}) (v int64, err error) {
	args := make([]interface{}, 0, len(keys)+1)
	args = append(args, destination)
	args = append(args, keys...)
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("SINTERSTORE", args...))
	conn.Close()
	return
}

// Returns if member is a member of the set stored at key.
//
// Boolean reply v, specifically:
// true if the element is a member of the set.
// false if the element is not a member of the set, or if key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) SISMEMBER(key string, member interface{}) (v bool, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Bool(conn.Do("SISMEMBER", key, member))
	conn.Close()
	return
}

// Returns all the members of the set value stored at key.
//
// Array reply v: all elements of the set.
//
// Time complexity: O(N) where N is the set cardinality.
func (cp *ConnPool) SMEMBERS(key string) (v []string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Strings(conn.Do("SMEMBERS", key))
	conn.Close()
	return
}

// Move member from the set at source to the set at destination.
// This operation is atomic.
//
// Boolean reply, specifically:
// true if the element is moved.
// false if the element is not a member of source and no operation was performed.
//
// Time complexity: O(1)
func (cp *ConnPool) SMOVE(source, destination string, member interface{}) (v bool, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Bool(conn.Do("SMOVE", source, destination, member))
	conn.Close()
	return
}

// Removes and returns one or more random elements from the set value store at key.
// The count argument is available since version 3.2.
//
// Array reply v: the removed elements.
// ErrNil when key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) SPOP(key string, count int) (v []string, err error) {
	conn := cp.GetMasterConn()
	if cp.lessThan("3.2.0") {
		if count > 1 {
			return nil, ErrNotSupport
		}
		var s string
		if s, err = lib.String(conn.Do("SPOP", key)); err != nil {
			v = []string{s}
		}
	} else {
		v, err = lib.Strings(conn.Do("SPOP", key, count))
	}
	conn.Close()
	if err == lib.ErrNil {
		err = ErrNil
	}
	return
}

// Return random elements from the set value stored at key.
//
// Array reply v: an array of elements, or an empty array when key does not exist.
//
// Time complexity: Without the count argument O(1), otherwise O(N) where N is
// the absolute value of the passed count.
func (cp *ConnPool) SRANDMEMBER(key string, count int) (v []string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Strings(conn.Do("SRANDMEMBER", key, count))
	conn.Close()
	return
}

// Remove the specified members from the set stored at key.
//
// Integer reply v: the number of members that were removed from the set,
// not including non existing members.
//
// Time complexity: O(N) where N is the number of members to be removed.
func (cp *ConnPool) SREM(key string, members ...interface{}) (v int64, err error) {
	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)
	args = append(args, members...)
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("SREM", args...))
	conn.Close()
	return
}

func (cp *ConnPool) SSCAN() {
	conn := cp.GetMasterConn()
	// @todo
	conn.Close()
	return
}

// Returns the members of the set resulting from the union of all the given sets.
//
// Array reply v: list with members of the resulting set.
//
// Time complexity: O(N) where N is the total number of elements in all given sets.
func (cp *ConnPool) SUNION(keys ...interface{}) (v []string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Strings(conn.Do("SUNION", keys...))
	conn.Close()
	return
}

// This command is equal to SUNION, but instead of returning the resulting
// set, it is stored in destination.
//
// Integer reply v: the number of elements in the resulting set.
//
// Time complexity: O(N) where N is the total number of elements in all given sets.
func (cp *ConnPool) SUNIONSTORE(destination string, keys ...interface{}) (v int64, err error) {
	args := make([]interface{}, 0, len(keys)+1)
	args = append(args, destination)
	args = append(args, keys...)
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("SUNIONSTORE", args...))
	conn.Close()
	return
}
