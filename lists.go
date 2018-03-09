package redis

import lib "github.com/garyburd/redigo/redis"

// BLPOP is a blocking list pop primitive.
//
// Array reply v: specifically:
// A two-element multi-bulk with the first element being the name of the key where
// an element was popped and the second element being the value of the popped element.
//
// Time complexity: O(1)
func (cp *ConnPool) BLPOP(timeout int64, keys ...interface{}) (v []string, err error) {
	args := make([]interface{}, 0, len(keys)+1)
	args = append(args, keys...)
	args = append(args, timeout)
	conn := cp.GetMasterConn()
	v, err = lib.Strings(conn.Do("BLPOP", args...))
	conn.Close()
	if err == lib.ErrNil {
		err = ErrNil
	}
	return
}

// BRPOP is a blocking list pop primitive.
//
// Array reply v: specifically:
// A two-element multi-bulk with the first element being the name of the key where
// an element was popped and the second element being the value of the popped element.
//
// Time complexity: O(1)
func (cp *ConnPool) BRPOP(timeout int64, keys ...interface{}) (v []string, err error) {
	args := make([]interface{}, 0, len(keys)+1)
	args = append(args, keys...)
	args = append(args, timeout)
	conn := cp.GetMasterConn()
	v, err = lib.Strings(conn.Do("BRPOP", args...))
	conn.Close()
	if err == lib.ErrNil {
		err = ErrNil
	}
	return
}

// BRPOPLPUSH is the blocking variant of RPOPLPUSH.
//
// Bulk string reply v: the element being popped from source and pushed to
// destination. If timeout is reached, a Null reply is returned.
// ErrNil when source does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) BRPOPLPUSH(source, destination string, timeout int64) (v string, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.String(conn.Do("BRPOPLPUSH", source, destination, timeout))
	conn.Close()
	if err == lib.ErrNil {
		err = ErrNil
	}
	return
}

// Returns the element at index index in the list stored at key.
//
// Bulk string reply v: the requested element.
// ErrNil when index is out of range.
//
// Time complexity: O(N) where N is the number of elements to traverse to get
// to the element at index. This makes asking for the first or the last element of
// the list O(1).
func (cp *ConnPool) LINDEX(key string, index interface{}) (v string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.String(conn.Do("LINDEX", key, index))
	conn.Close()
	if err == lib.ErrNil {
		err = ErrNil
	}
	return
}

// Inserts value in the list stored at key either before or after the reference value pivot.
//
// Integer reply v: the length of the list after the insert operation, or -1 when the value pivot was not found.
//
// Time complexity: O(N) where N is the number of elements to traverse before
// seeing the value pivot. This means that inserting somewhere on the left end on
// the list (head) can be considered O(1) and inserting somewhere on the right end (tail) is O(N).
func (cp *ConnPool) LINSERT(key string, before bool, pivot, value interface{}) (v int64, err error) {
	pos := "AFTER"
	if before {
		pos = "BEFORE"
	}
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("LINSERT", key, pos, pivot, value))
	conn.Close()
	return
}

// Returns the length of the list stored at key.
//
// Integer reply v: the length of the list at key.
//
// Time complexity: O(1)
func (cp *ConnPool) LLEN(key string) (v int64, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Int64(conn.Do("LLEN", key))
	conn.Close()
	return
}

// Removes and returns the first element of the list stored at key.
//
// Bulk string reply v: the value of the first element.
// ErrNil when key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) LPOP(key string) (v string, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.String(conn.Do("LPOP", key))
	conn.Close()
	if err == lib.ErrNil {
		err = ErrNil
	}
	return
}

// Insert all the specified values at the head of the list stored at key.
//
// Integer reply v: the length of the list after the push operations.
//
// Time complexity: O(1)
func (cp *ConnPool) LPUSH(key string, values ...interface{}) (v int64, err error) {
	args := make([]interface{}, 0, len(values)+1)
	args = append(args, key)
	args = append(args, values...)
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("LPUSH", args...))
	conn.Close()
	return
}

// Inserts value at the head of the list stored at key, only if key already exists
// and holds a list.
//
// Integer reply v: the length of the list after the push operation.
//
// Time complexity: O(1)
func (cp *ConnPool) LPUSHX(key string, value interface{}) (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("LPUSHX", key, value))
	conn.Close()
	return
}

// Returns the specified elements of the list stored at key.
//
// Array reply v: list of elements in the specified range.
//
// Time complexity: O(S+N) where S is the distance of start offset from HEAD for
// small lists, from nearest end (HEAD or TAIL) for large lists; and N is the number
// of elements in the specified range.
func (cp *ConnPool) LRANGE(key string, start, stop interface{}) (v []string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Strings(conn.Do("LRANGE", key, start, stop))
	conn.Close()
	return
}

// Removes the first count occurrences of elements equal to value from the
// list stored at key. The count argument influences the operation in the following ways:
// count > 0: Remove elements equal to value moving from head to tail.
// count < 0: Remove elements equal to value moving from tail to head.
// count = 0: Remove all elements equal to value.
//
// Integer reply v: the number of removed elements.
//
// Time complexity: O(N) where N is the length of the list.
func (cp *ConnPool) LREM(key string, count int, value interface{}) (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("LREM", key, count, value))
	conn.Close()
	return
}

// Sets the list element at index to value.
//
// Time complexity: O(N) where N is the length of the list. Setting either the first
// or the last element of the list is O(1).
func (cp *ConnPool) LSET(key string, index, value interface{}) (err error) {
	conn := cp.GetMasterConn()
	_, err = conn.Do("LSET", key, index, value)
	conn.Close()
	return
}

// Trim an existing list so that it will contain only the specified range of elements specified.
//
// Time complexity: O(N) where N is the number of elements to be removed by the operation.
func (cp *ConnPool) LTRIM(key string, start, stop interface{}) (err error) {
	conn := cp.GetMasterConn()
	_, err = conn.Do("LTRIM", key, start, stop)
	conn.Close()
	return
}

// Removes and returns the last element of the list stored at key.
//
// Bulk string reply v: the value of the last element.
// ErrNil when key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) RPOP(key string) (v string, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.String(conn.Do("RPOP", key))
	conn.Close()
	if err == lib.ErrNil {
		err = ErrNil
	}
	return
}

// Atomically returns and removes the last element (tail) of the list stored at
// source, and pushes the element at the first element (head) of the list
// stored at destination.
//
// Bulk string reply v: the element being popped and pushed.
// ErrNil when source does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) RPOPLPUSH(source, destination string) (v string, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.String(conn.Do("RPOPLPUSH", source, destination))
	conn.Close()
	if err == lib.ErrNil {
		err = ErrNil
	}
	return
}

// Insert all the specified values at the tail of the list stored at key.
//
// Integer reply v: the length of the list after the push operation.
//
// Time complexity: O(1)
func (cp *ConnPool) RPUSH(key string, values ...interface{}) (v int64, err error) {
	args := make([]interface{}, 0, len(values)+1)
	args = append(args, key)
	args = append(args, values...)
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("RPUSH", args...))
	conn.Close()
	return
}

// Inserts value at the tail of the list stored at key, only if key already exists and holds a list.
//
// Integer reply v: the length of the list after the push operation.
//
// Time complexity: O(1)
func (cp *ConnPool) RPUSHX(key string, value interface{}) (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("RPUSHX", key, value))
	conn.Close()
	return
}
