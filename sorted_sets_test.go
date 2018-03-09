package redis

import (
	"testing"
	"time"
)

func TestConnPool_ZADD_ZREM(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_ZADD_ZREM"
	member := randValue()
	if v, err := p.ZREM(key, member); err != nil || v > 0 {
		t.Fail()
	}
	if v, err := p.ZADD(key, time.Now().Unix(), member); err != nil || v != 1 {
		t.Fail()
	}
	if v, err := p.ZREM(key, member); err != nil || v != 1 {
		t.Fail()
	}
	m := make(map[string]interface{}, 10)
	ks := make([]interface{}, 0, 10)
	for i := 0; i < 10; i++ {
		k := "key_" + randString(i+1)
		m[k] = time.Now().UnixNano()
		ks = append(ks, k)
	}
	if v, err := p.ZADDMap(key, m); err != nil || int(v) != len(m) {
		t.Fail()
	}
	if v, err := p.ZREM(key, ks...); err != nil || int(v) != len(m) {
		t.Fail()
	}
}

func TestConnPool_ZADDExtra_ZADDExtraMap(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_ZADDExtra"
	mem := "extra"
	if v, err := p.ZADDExtra(key, UpdateOnly, false, MaxScore, mem); err != nil || v != 0 {
		t.Fail()
	}
	if v, err := p.ZADDExtra(key, AddOnly, false, MaxScore, mem); err != nil || v != 1 {
		t.Fail()
	}
	if v, err := p.ZADDExtra(key, AddOnly, false, MinScore, mem); err != nil || v != 0 {
		t.Fail()
	}
	if v, err := p.ZADDExtra(key, UpdateOnly, false, MinScore, mem); err != nil || v != 0 {
		t.Fail()
	}
	if v, err := p.ZADDExtra(key, UpdateOnly, true, MaxScore, mem); err != nil || v != 1 {
		t.Fail()
	}
	m := make(map[string]interface{}, 10)
	for i := 0; i < 10; i++ {
		k := "key_" + randString(i+1)
		m[k] = time.Now().UnixNano()
	}
	key = "TestConnPool_ZADDExtraMap"
	if v, err := p.ZADDExtraMap(key, UpdateOnly, false, m); err != nil || v != 0 {
		t.Fail()
	}
	if v, err := p.ZADDExtraMap(key, AddOnly, false, m); err != nil || int(v) != len(m) {
		t.Fail()
	}
	if v, err := p.ZADDExtraMap(key, AddOnly, false, m); err != nil || v != 0 {
		t.Fail()
	}
	if v, err := p.ZADDExtraMap(key, UpdateOnly, false, m); err != nil || v != 0 {
		t.Fail()
	}
	for k, _ := range m {
		m[k] = time.Now().UnixNano()
	}
	if v, err := p.ZADDExtraMap(key, UpdateOnly, true, m); err != nil || int(v) != len(m) {
		t.Fail()
	}
}

func TestConnPool_ZINCRBY_ZADDIncr(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_ZINCRBY_ZADDIncr"
	mem1 := "zincrby"
	mem2 := "minimum"
	if v, err := p.ZINCRBY(key, 1, mem1); err != nil || v != "1" {
		t.Fail()
	}
	if v, err := p.ZADDIncr(key, Default, 9, mem1); err != nil || v != "10" {
		t.Fail()
	}
	if v, err := p.ZINCRBY(key, 10, mem1); err != nil || v != "20" {
		t.Fail()
	}
	if v, err := p.ZINCRBY(key, MaxScore, mem1); err != nil || v != MaxScore {
		t.Fail()
	}
	if v, err := p.ZINCRBY(key, MinScore, mem2); err != nil || v != MinScore {
		t.Fail()
	}
}

func TestConnPool_ZCARD_ZCOUNT(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_ZCARD_ZCOUNT"
	m := make(map[string]interface{}, 20)
	for i := 0; i < 20; i++ {
		m[randString(i+1)] = i
	}
	if v, err := p.ZCARD(key); err != nil || v != 0 {
		t.Fail()
	}
	if v, err := p.ZCOUNT(key, MinScore, MaxScore); err != nil || v != 0 {
		t.Fail()
	}
	if v, err := p.ZADDMap(key, m); err != nil || int(v) != len(m) {
		t.Fail()
	}
	if v, err := p.ZCARD(key); err != nil || int(v) != len(m) {
		t.Fail()
	}
	if v, err := p.ZCOUNT(key, 0, 9); err != nil || v != 10 {
		t.Fail()
	}
}

func TestConnPool_ZLEXCOUNT(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_ZLEXCOUNT"
	if v, err := p.ZLEXCOUNT(key, MinLex, MaxLex); err != nil || v != 0 {
		t.Fail()
	}
	m := make(map[string]interface{}, 20)
	for i := 0; i < 20; i++ {
		k := string([]byte{byte(65 + i)})
		m[k] = MaxScore
	}
	if v, err := p.ZADDMap(key, m); err != nil || int(v) != len(m) {
		t.Fail()
	}
	if v, err := p.ZLEXCOUNT(key, MinLex, MaxLex); err != nil || int(v) != len(m) {
		t.Fail()
	}
	if v, err := p.ZLEXCOUNT(key, "[A", "[F"); err != nil || v != 6 {
		t.Fail()
	}
}

func TestConnPool_ZSCORE_ZRANK_ZREVRANK(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_ZSCORE_ZRANK_ZREVRANK"
	mem := "test"
	if _, err := p.ZSCORE(key, mem); err != ErrNil {
		t.Fail()
	}
	if _, err := p.ZRANK(key, mem); err != ErrNil {
		t.Fail()
	}
	if _, err := p.ZREVRANK(key, mem); err != ErrNil {
		t.Fail()
	}
	if v, err := p.ZADD(key, MaxScore, mem); err != nil || v != 1 {
		t.Fail()
	}
	if v, err := p.ZSCORE(key, mem); err != nil || v != MaxScore {
		t.Fail()
	}
	if v, err := p.ZRANK(key, mem); err != nil || v != 0 {
		t.Fail()
	}
	if v, err := p.ZREVRANK(key, mem); err != nil || v != 0 {
		t.Fail()
	}
}

func TestConnPool_ZRANGE_ZREVRANGE(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_ZRANGE_ZREVRANGE"
	m := make(map[string]interface{}, 20)
	for i := 0; i < 20; i++ {
		k := string([]byte{byte(65 + i)})
		m[k] = i
	}
	if v, err := p.ZADDMap(key, m); err != nil || int(v) != len(m) {
		t.Fail()
	}
	if v, err := p.ZRANGE(key, 0, 9); err != nil || len(v) != 10 {
		t.Fail()
	}
	if v, err := p.ZRANGEWithScores(key, 10, 19); err != nil || len(v) != 10 {
		t.Fail()
	}
	if v, err := p.ZREVRANGE(key, 0, 9); err != nil || len(v) != 10 {
		t.Fail()
	}
	if v, err := p.ZREVRANGEWithScores(key, 10, 19); err != nil || len(v) != 10 {
		t.Fail()
	}
}

func TestConnPool_ZREMRANGEBYRANK(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_ZREMRANGEBYRANK"
	m := make(map[string]interface{}, 20)
	for i := 0; i < 20; i++ {
		k := string([]byte{byte(65 + i)})
		m[k] = i
	}
	if v, err := p.ZADDMap(key, m); err != nil || int(v) != len(m) {
		t.Fail()
	}
	if v, err := p.ZREMRANGEBYRANK(key, 0, 9); err != nil || int(v) != 10 {
		t.Fail()
	}
	if v, err := p.ZCARD(key); err != nil || v != 10 {
		t.Fail()
	}
}

func TestConnPool_ZRANGEBYLEX_ZREVRANGEBYLEX_ZREMRANGEBYLEX(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_ZRANGEBYLEX_ZREVRANGEBYLEX_ZREMRANGEBYLEX"
	m := make(map[string]interface{}, 20)
	for i := 0; i < 20; i++ {
		k := string([]byte{byte(65 + i)})
		m[k] = MaxScore
	}
	if v, err := p.ZADDMap(key, m); err != nil || int(v) != len(m) {
		t.Fail()
	}
	if v, err := p.ZRANGEBYLEX(key, MinLex, "[F", 0, 20); err != nil || len(v) != 6 {
		t.Fail()
	}
	if v, err := p.ZREVRANGEBYLEX(key, MaxLex, "(N", 0, 20); err != nil || len(v) != 6 {
		t.Fail()
	}
	if v, err := p.ZREMRANGEBYLEX(key, "(G", "[M"); err != nil || int(v) != 6 {
		t.Fail()
	}
	if v, err := p.ZCARD(key); err != nil || v != 14 {
		t.Fail()
	}
}

func TestConnPool_ZRANGEBYSCORE_ZREVRANGEBYSCORE_ZREMRANGEBYSCORE(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_ZRANGEBYSCORE_ZREVRANGEBYSCORE_ZREMRANGEBYSCORE"
	m := make(map[string]interface{}, 20)
	for i := 0; i < 20; i++ {
		k := string([]byte{byte(65 + i)})
		m[k] = i
	}
	if v, err := p.ZADDMap(key, m); err != nil || int(v) != len(m) {
		t.Fail()
	}
	if v, err := p.ZRANGEBYSCORE(key, 0, 9, 0, 20); err != nil || len(v) != 10 {
		t.Fail()
	}
	if v, err := p.ZRANGEBYSCOREWithScores(key, 0, 9, 0, 20); err != nil || len(v) != 10 {
		t.Fail()
	}
	if v, err := p.ZREVRANGEBYSCORE(key, 19, 10, 0, 20); err != nil || len(v) != 10 {
		t.Fail()
	}
	if v, err := p.ZREVRANGEBYSCOREWithScores(key, 19, 10, 0, 20); err != nil || len(v) != 10 {
		t.Fail()
	}
	if v, err := p.ZREMRANGEBYSCORE(key, 0, 9); err != nil || v != 10 {
		t.Fail()
	}
	if v, err := p.ZCARD(key); err != nil || v != 10 {
		t.Fail()
	}
}
