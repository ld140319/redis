package redis

import lib "github.com/garyburd/redigo/redis"

const (
	MaxScore = "inf"
	MinScore = "-inf"

	MaxLex = "+"
	MinLex = "-"
)

// Adds the specified member with the specified score to the sorted set stored at key.
//
// Integer reply v, specifically: The number of elements added to the sorted sets,
// not including elements already existing for which the score was updated.
//
// Time complexity: O(log(N)) for each item added, where N is the number of elements in the sorted set.
func (cp *ConnPool) ZADD(key string, score, member interface{}) (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("ZADD", key, score, member))
	conn.Close()
	return
}

// Adds multiple member / score pairs.
// Map key is the member and value is the specified score.
func (cp *ConnPool) ZADDMap(key string, m map[string]interface{}) (v int64, err error) {
	args := make([]interface{}, 0, len(m)*2+1)
	args = append(args, key)
	for member, score := range m {
		args = append(args, score, member)
	}
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("ZADD", args...))
	conn.Close()
	return
}

// Available since 3.0.2.
//
// Adds to the sorted set with INCR option.
func (cp *ConnPool) ZADDIncr(key string, option Option, score, member interface{}) (v string, err error) {
	if cp.lessThan("3.0.2") {
		return "", ErrNotSupport
	}
	args := make([]interface{}, 0, 5)
	args = append(args, key)
	if option == AddOnly {
		args = append(args, "NX")
	} else if option == UpdateOnly {
		args = append(args, "XX")
	}
	args = append(args, "INCR")
	args = append(args, score, member)
	conn := cp.GetMasterConn()
	v, err = lib.String(conn.Do("ZADD", args...))
	conn.Close()
	return
}

// Available since 3.0.2.
//
// Adds to the sorted set with extra options.
func (cp *ConnPool) ZADDExtra(key string, option Option, ch bool, score, member interface{}) (v int64, err error) {
	if cp.lessThan("3.0.2") {
		return 0, ErrNotSupport
	}
	args := make([]interface{}, 0, 5)
	args = append(args, key)
	if option == AddOnly {
		args = append(args, "NX")
	} else if option == UpdateOnly {
		args = append(args, "XX")
	}
	if ch {
		args = append(args, "CH")
	}
	args = append(args, score, member)
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("ZADD", args...))
	conn.Close()
	return
}

// Available since 3.0.2.
//
// Adds multiple score / member pairs with extra options.
func (cp *ConnPool) ZADDExtraMap(key string, option Option, ch bool, m map[string]interface{}) (v int64, err error) {
	if cp.lessThan("3.0.2") {
		return 0, ErrNotSupport
	}
	args := make([]interface{}, 0, len(m)*2+3)
	args = append(args, key)
	if option == AddOnly {
		args = append(args, "NX")
	} else if option == UpdateOnly {
		args = append(args, "XX")
	}
	if ch {
		args = append(args, "CH")
	}
	for member, score := range m {
		args = append(args, score, member)
	}
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("ZADD", args...))
	conn.Close()
	return
}

// Returns the sorted set cardinality (number of elements) of the sorted set stored at key.
//
// Integer reply v: the cardinality (number of elements) of the sorted set, or 0 if key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) ZCARD(key string) (v int64, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Int64(conn.Do("ZCARD", key))
	conn.Close()
	return
}

// Returns the number of elements in the sorted set at key with a score between min and max.
//
// Integer reply v: the number of elements in the specified score range.
//
// Time complexity: O(log(N)) with N being the number of elements in the sorted set.
func (cp *ConnPool) ZCOUNT(key string, min, max interface{}) (v int64, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Int64(conn.Do("ZCOUNT", key, min, max))
	conn.Close()
	return
}

// Increments the score of member in the sorted set stored at key by increment.
//
// Bulk string reply v: the new score of member (a double precision floating point
// number), represented as string.
// inf and -inf are valid scores.
//
// Time complexity: O(log(N)) where N is the number of elements in the sorted set.
func (cp *ConnPool) ZINCRBY(key string, increment, member interface{}) (v string, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.String(conn.Do("ZINCRBY", key, increment, member))
	conn.Close()
	return
}

func (cp *ConnPool) ZINTERSTORE() {
	conn := cp.GetMasterConn()
	// @todo
	conn.Close()
	return
}

// Available since 2.8.9.
//
// When all the elements in a sorted set are inserted with the same score, in
// order to force lexicographical ordering, this command returns the number
// of elements in the sorted set at key with a value between min and max.
//
// Valid min and max must start with ( or [, in order to specify if the range
// item is respectively exclusive or inclusive.
//
// Integer reply v: the number of elements in the specified score range.
//
// Time complexity: O(log(N)) with N being the number of elements in the sorted set.
func (cp *ConnPool) ZLEXCOUNT(key string, min, max interface{}) (v int64, err error) {
	if cp.lessThan("2.8.9") {
		return 0, ErrNotSupport
	}
	conn := cp.GetSlaveConn()
	v, err = lib.Int64(conn.Do("ZLEXCOUNT", key, min, max))
	conn.Close()
	return
}

// Returns the specified range of elements in the sorted set stored at key.
//
// Array reply: list of elements in the specified range.
//
// Time complexity: O(log(N)+M) with N being the number of elements in the
// sorted set and M the number of elements returned.
func (cp *ConnPool) ZRANGE(key string, start, stop interface{}) (v []string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Strings(conn.Do("ZRANGE", key, start, stop))
	conn.Close()
	return
}

// Map reply: elements and scores in the specified range.
func (cp *ConnPool) ZRANGEWithScores(key string, start, stop interface{}) (v map[string]string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.StringMap(conn.Do("ZRANGE", key, start, stop, "WITHSCORES"))
	conn.Close()
	return
}

// Available since 2.8.9.
//
// When all the elements in a sorted set are inserted with the same score, in
// order to force lexicographical ordering, this command returns all the
// elements in the sorted set at key with a value between min and max.
//If the elements in the sorted set have different scores, the returned elements are unspecified.
//
// Array reply v: list of elements in the specified score range.
//
// Time complexity: O(log(N)+M) with N being the number of elements in the
// sorted set and M the number of elements being returned. If M is constant (e.g.
// always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
func (cp *ConnPool) ZRANGEBYLEX(key string, min, max, offset, count interface{}) (v []string, err error) {
	if cp.lessThan("2.8.9") {
		return nil, ErrNotSupport
	}
	conn := cp.GetSlaveConn()
	v, err = lib.Strings(conn.Do("ZRANGEBYLEX", key, min, max, "LIMIT", offset, count))
	conn.Close()
	return
}

// Returns all the elements in the sorted set at key with a score between min
// and max (including elements with score equal to min or max). The elements
// are considered to be ordered from low to high scores.
//
// Array reply v: list of elements in the specified score range.
//
// Time complexity: O(log(N)+M) with N being the number of elements in the
// sorted set and M the number of elements being returned. If M is constant (e.g.
// always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
func (cp *ConnPool) ZRANGEBYSCORE(key string, min, max, offset, count interface{}) (v []string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Strings(conn.Do("ZRANGEBYSCORE", key, min, max, "LIMIT", offset, count))
	conn.Close()
	return
}

// Map reply: elements and scores in the specified range.
func (cp *ConnPool) ZRANGEBYSCOREWithScores(key string, min, max, offset, count interface{}) (v map[string]string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.StringMap(conn.Do("ZRANGEBYSCORE", key, min, max, "WITHSCORES", "LIMIT", offset, count))
	conn.Close()
	return
}

// Returns the rank of member in the sorted set stored at key, with the scores
// ordered from low to high. The rank (or index) is 0-based, which means that
// the member with the lowest score has rank 0.
//
// Integer replyv : the rank of member.
// ErrNil if member does not exist in the sorted set or key does not exist.
//
// Time complexity: O(log(N))
func (cp *ConnPool) ZRANK(key string, member interface{}) (v int64, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Int64(conn.Do("ZRANK", key, member))
	conn.Close()
	if err == lib.ErrNil {
		err = ErrNil
	}
	return
}

// Removes the specified members from the sorted set stored at key.
// Non existing members are ignored.
//
// Integer reply v, specifically:
// The number of members removed from the sorted set, not including non existing members.
//
// Time complexity: O(M*log(N)) with N being the number of elements in the
// sorted set and M the number of elements to be removed.
func (cp *ConnPool) ZREM(key string, members ...interface{}) (v int64, err error) {
	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)
	args = append(args, members...)
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("ZREM", args...))
	conn.Close()
	return
}

// Available since 2.8.9.
//
// When all the elements in a sorted set are inserted with the same score, in
// order to force lexicographical ordering, this command removes all elements
// in the sorted set stored at key between the lexicographical range specified by min and max.
//
// Integer reply v: the number of elements removed.
//
// Time complexity: O(log(N)+M) with N being the number of elements in the
// sorted set and M the number of elements removed by the operation.
func (cp *ConnPool) ZREMRANGEBYLEX(key string, min, max interface{}) (v int64, err error) {
	if cp.lessThan("2.8.9") {
		return 0, ErrNotSupport
	}
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("ZREMRANGEBYLEX", key, min, max))
	conn.Close()
	return
}

// Removes all elements in the sorted set stored at key with rank between start and stop.
//
// Integer reply v: the number of elements removed.
//
// Time complexity: O(log(N)+M) with N being the number of elements in the
// sorted set and M the number of elements removed by the operation.
func (cp *ConnPool) ZREMRANGEBYRANK(key string, start, stop interface{}) (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("ZREMRANGEBYRANK", key, start, stop))
	conn.Close()
	return
}

// Removes all elements in the sorted set stored at key with a score between min and max (inclusive).
//
// Integer reply v: the number of elements removed.
//
// Time complexity: O(log(N)+M) with N being the number of elements in the
// sorted set and M the number of elements removed by the operation.
func (cp *ConnPool) ZREMRANGEBYSCORE(key string, min, max interface{}) (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("ZREMRANGEBYSCORE", key, min, max))
	conn.Close()
	return
}

// Returns the specified range of elements in the sorted set stored at key. The
// elements are considered to be ordered from the highest to the lowest score.
//
// Array reply v: list of elements in the specified range.
//
// Time complexity: O(log(N)+M) with N being the number of elements in the
// sorted set and M the number of elements returned.
func (cp *ConnPool) ZREVRANGE(key string, start, stop interface{}) (v []string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Strings(conn.Do("ZREVRANGE", key, start, stop))
	conn.Close()
	return
}

// Map reply: elements and scores in the specified range.
func (cp *ConnPool) ZREVRANGEWithScores(key string, start, stop interface{}) (v map[string]string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.StringMap(conn.Do("ZREVRANGE", key, start, stop, "WITHSCORES"))
	conn.Close()
	return
}

// Available since 2.8.9.
//
// When all the elements in a sorted set are inserted with the same score, in
// order to force lexicographical ordering, this command returns all the
// elements in the sorted set at key with a value between max and min.
//
// Array reply v: list of elements in the specified score range.
//
// Time complexity: O(log(N)+M) with N being the number of elements in the
// sorted set and M the number of elements being returned. If M is constant (e.g.
// always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
func (cp *ConnPool) ZREVRANGEBYLEX(key string, max, min, offset, count interface{}) (v []string, err error) {
	if cp.lessThan("2.8.9") {
		return nil, ErrNotSupport
	}
	conn := cp.GetSlaveConn()
	v, err = lib.Strings(conn.Do("ZREVRANGEBYLEX", key, max, min, "LIMIT", offset, count))
	conn.Close()
	return
}

// Returns all the elements in the sorted set at key with a score between max
// and min (including elements with score equal to max or min). In contrary to
// the default ordering of sorted sets, for this command the elements are
// considered to be ordered from high to low scores.
//
// Array reply: list of elements in the specified score range.
//
// Time complexity: O(log(N)+M) with N being the number of elements in the
// sorted set and M the number of elements being returned. If M is constant (e.g.
// always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
func (cp *ConnPool) ZREVRANGEBYSCORE(key string, max, min, offset, count interface{}) (v []string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Strings(conn.Do("ZREVRANGEBYSCORE", key, max, min, "LIMIT", offset, count))
	conn.Close()
	return
}

// Map reply: elements and scores in the specified range.
func (cp *ConnPool) ZREVRANGEBYSCOREWithScores(key string, max, min, offset, count interface{}) (v map[string]string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.StringMap(conn.Do("ZREVRANGEBYSCORE", key, max, min, "WITHSCORES", "LIMIT", offset, count))
	conn.Close()
	return
}

// Returns the rank of member in the sorted set stored at key, with the scores
// ordered from high to low. The rank (or index) is 0-based, which means that
// the member with the highest score has rank 0.
//
// Integer reply v: the rank of member.
// ErrNil if member does not exist in the sorted set or key does not exist.
//
// Time complexity: O(log(N))
func (cp *ConnPool) ZREVRANK(key string, member interface{}) (v int64, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.Int64(conn.Do("ZREVRANK", key, member))
	conn.Close()
	if err == lib.ErrNil {
		err = ErrNil
	}
	return
}

func (cp *ConnPool) ZSCAN() {
	conn := cp.GetMasterConn()
	// @todo
	conn.Close()
	return
}

// Returns the score of member in the sorted set at key.
//
// Bulk string reply v: the new score of member (a double precision floating point
// number), represented as string.
// ErrNil if member does not exist in the sorted set or key does not exist.
//
// Time complexity: O(1)
func (cp *ConnPool) ZSCORE(key string, member interface{}) (v string, err error) {
	conn := cp.GetSlaveConn()
	v, err = lib.String(conn.Do("ZSCORE", key, member))
	conn.Close()
	if err == lib.ErrNil {
		err = ErrNil
	}
	return
}

// Time complexity: O(N)+O(M log(M)) with N being the sum of the sizes of the
// input sorted sets, and M being the number of elements in the resulting sorted set.
func (cp *ConnPool) ZUNIONSTORE() {
	conn := cp.GetMasterConn()
	// @todo
	conn.Close()
	return
}
