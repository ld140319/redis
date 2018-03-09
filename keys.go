package redis

import lib "github.com/garyburd/redigo/redis"

type KeyType string

const (
	NoneType      = "none"
	StringType    = "string"
	HashType      = "hash"
	ListType      = "list"
	SetType       = "set"
	SortedSetType = "zset"
)

// Removes the specified keys. A key is ignored if it does not exist.
//
// Integer reply v: The number of keys that were removed.
//
// Time complexity: O(N) where N is the number of keys that will be removed.
// When a key to remove holds a value other than a string, the individual
// complexity for this key is O(M) where M is the number of elements in the list,
// set, sorted set or hash. Removing a single key that holds a string value is O(1).
func (cp *ConnPool) DEL(keys ...interface{}) (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("DEL", keys...))
	conn.Close()
	return
}

// Serialize the value stored at key in a Redis-specific format and return it to
// the user. The returned value can be synthesized back into a Redis key using
// the RESTORE command.
//
// Bulk string reply v: the serialized value.
// ErrNil when key does not exist.
//
// Time complexity: O(1) to access the key and additional O(N*M) to serialized it,
// where N is the number of Redis objects composing the value and M their
// average size. For small string values the time complexity is thus O(1)+O(1*M)
// where M is small, so simply O(1).
func (cp *ConnPool) DUMP(key string) (v string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.String(conn.Do("DUMP", key))
	conn.Close()
	if err == lib.ErrNil {
		err = ErrNil
	}
	return
}

// Returns if key exists.
//
// Boolean reply v, specifically:
// true if the key exists.
// false if the key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) EXISTS(key string) (v bool, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Bool(conn.Do("EXISTS", key))
	conn.Close()
	return
}

// Available since 3.0.3.
//
// Integer reply v, specifically:
// The number of keys existing among the ones specified as arguments.
// Keys mentioned multiple times and existing are counted multiple times.
func (cp *ConnPool) EXISTSExtra(keys ...interface{}) (v int64, err error) {
	if cp.lessThan("3.0.3") {
		return 0, ErrNotSupport
	}
	conn := cp.GetSlaveConn()
	v, err = lib.Int64(conn.Do("EXISTS", keys...))
	conn.Close()
	return
}

// Set a timeout on key.
//
// Boolean reply v, specifically:
// true if the timeout was set.
// false if key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) EXPIRE(key string, seconds interface{}) (v bool, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Bool(conn.Do("EXPIRE", key, seconds))
	conn.Close()
	return
}

// EXPIREAT has the same effect and semantic as EXPIRE, but instead of
// specifying the number of seconds representing the TTL (time to live), it
// takes an absolute Unix timestamp (seconds since January 1, 1970).
//
// Boolean reply v, specifically:
// true if the timeout was set.
// false if key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) EXPIREAT(key string, timestamp interface{}) (v bool, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Bool(conn.Do("EXPIREAT", key, timestamp))
	conn.Close()
	return
}

// Returns all keys matching pattern.
//
// Array reply v: list of keys matching pattern.
//
// Time complexity: O(N) with N being the number of keys in the database,
// under the assumption that the key names in the database and the given
// pattern have limited length.
func (cp *ConnPool) KEYS(pattern interface{}) (v []string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Strings(conn.Do("KEYS", pattern))
	conn.Close()
	return
}

func (cp *ConnPool) MIGRATE() {
	conn := cp.GetMasterConn()
	// @todo
	conn.Close()
	return
}

func (cp *ConnPool) MOVE() {
	conn := cp.GetMasterConn()
	// @todo
	conn.Close()
	return
}

func (cp *ConnPool) OBJECT() {
	conn := cp.GetMasterConn()
	// @todo
	conn.Close()
	return
}

// Remove the existing timeout on key, turning the key from volatile (a key
// with an expire set) to persistent (a key that will never expire as no timeout is associated).
//
// Boolean reply v, specifically:
// true if the timeout was removed.
// false if key does not exist or does not have an associated timeout.
//
// Time complexity: O(1)
func (cp *ConnPool) PERSIST(key string) (v bool, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Bool(conn.Do("PERSIST", key))
	conn.Close()
	return
}

// This command works exactly like EXPIRE but the time to live of the key is
// specified in milliseconds instead of seconds.
//
// Boolean reply v, specifically:
// true if the timeout was set.
// false if key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) PEXPIRE(key string, milliseconds interface{}) (v bool, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Bool(conn.Do("PEXPIRE", key, milliseconds))
	conn.Close()
	return
}

// PEXPIREAT has the same effect and semantic as EXPIREAT, but the Unix time
// at which the key will expire is specified in milliseconds instead of seconds.
//
// Boolean reply v, specifically:
// true if the timeout was set.
// false if key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) PEXPIREAT(key string, milliTimestamp interface{}) (v bool, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Bool(conn.Do("PEXPIREAT", key, milliTimestamp))
	conn.Close()
	return
}

// Like TTL this command returns the remaining time to live of a key that has
// an expire set, with the sole difference that TTL returns the amount of
// remaining time in seconds while PTTL returns it in milliseconds.
//
// Integer reply v: TTL in milliseconds, or a negative value in order to signal an error.
// if > 2.8:
// -2 if the key does not exist.
// -1 if the key exists but has no associated expire.
//
// Time complexity: O(1)
func (cp *ConnPool) PTTL(key string) (v int64, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Int64(conn.Do("PTTL", key))
	conn.Close()
	return
}

// Return a random key from the currently selected database.
//
// Bulk string reply v: the random key.
// ErrNil when the database is empty.
//
// Time complexity: O(1)
func (cp *ConnPool) RANDOMKEY() (v string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.String(conn.Do("RANDOMKEY"))
	conn.Close()
	if err == lib.ErrNil {
		err = ErrNil
	}
	return
}

// Renames key to newkey.
//
// It returns an error when key does not exist.
// if < 3.2.0, an error is returned if source and destination names are the same.
//
// Time complexity: O(1)
func (cp *ConnPool) RENAME(key string, newKey string) error {
	conn := cp.GetMasterConn()
	_, err := conn.Do("RENAME", key, newKey)
	conn.Close()
	return err
}

// Renames key to newkey if newkey does not yet exist.
//
// Boolean reply v, specifically:
// true if key was renamed to newkey.
// false if newkey already exists.
// if < 3.2.0, an error is returned if source and destination names are the same.
//
// Time complexity: O(1)
func (cp *ConnPool) RENAMENX(key string, newKey string) (v bool, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Bool(conn.Do("RENAMENX", key, newKey))
	conn.Close()
	return
}

// Create a key associated with a value that is obtained by deserializing the
// provided serialized value (obtained via DUMP).
//
// It returns an error if RESTORE fails.
//
// Time complexity: O(1) to create the new key and additional O(N*M) to
// reconstruct the serialized value, where N is the number of Redis objects
// composing the value and M their average size. For small string values the time
// complexity is thus O(1)+O(1*M) where M is small, so simply O(1). However for
// sorted set values the complexity is O(N*M*log(N)) because inserting values into
// sorted sets is O(log(N)).
func (cp *ConnPool) RESTORE(key string, ttl /*milliseconds*/, value interface{}, replace bool) (err error) {
	conn := cp.GetMasterConn()
	if cp.greaterThan("3.0") && replace {
		_, err = conn.Do("RESTORE", key, ttl, value, "REPLACE")
	} else {
		_, err = conn.Do("RESTORE", key, ttl, value)
	}
	conn.Close()
	return err
}

func (cp *ConnPool) SCAN() {
	conn := cp.GetMasterConn()
	// @todo
	conn.Close()
	return
}

func (cp *ConnPool) SORT() {
	conn := cp.GetMasterConn()
	// @todo
	conn.Close()
	return
}

// Available since 3.2.1.
//
// Alters the last access time of a key(s). A key is ignored if it does not exist.
//
// Integer reply v: The number of keys that were touched.
//
// Time complexity: O(N) where N is the number of keys that will be touched.
func (cp *ConnPool) TOUCH(keys ...interface{}) (v int64, err error) {
	if cp.lessThan("3.2.1") {
		return 0, ErrNotSupport
	}
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("TOUCH", keys...))
	conn.Close()
	return
}

// Returns the remaining time to live of a key that has a timeout.
//
// Integer reply v: TTL in seconds, or a negative value in order to signal an error.
// -2 if the key does not exist.
// -1 if the key exists but has no associated expire.
//
// Time complexity: O(1)
func (cp *ConnPool) TTL(key string) (v int64, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Int64(conn.Do("TTL", key))
	conn.Close()
	return
}

// Returns the string representation of the type of the value stored at key.
//
// KeyType v: type of key, NoneType when key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) TYPE(key string) (v KeyType, err error) {
	conn := cp.GetMasterConn()
	var s string
	s, err = lib.String(conn.Do("TYPE", key))
	conn.Close()
	v = KeyType(s)
	return
}

// Available since 4.0.0.
func (cp *ConnPool) UNLINK(keys ...interface{}) (v int64, err error) {
	if cp.lessThan("4.0.0") {
		return 0, ErrNotSupport
	}
	// @todo
	return
}

func (cp *ConnPool) WAIT() {
	// @todo
}
