package redis

import (
	"fmt"
	"testing"
)

func TestPool_SADD_SCARD_SREM(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestPool_SADD_SCARD_SREM"
	vals := make([]interface{}, 0, 20)
	for i := 0; i < 20; i++ {
		vals = append(vals, randValue())
	}
	if v, err := p.SADD(key, vals...); err != nil || v != 20 {
		t.Fail()
	}
	if v, err := p.SCARD(key); err != nil || v != 20 {
		t.Fail()
	}
	if v, err := p.SREM(key, vals...); err != nil || v != 20 {
		t.Fail()
	}
	if v, err := p.SCARD(key); err != nil || v != 0 {
		t.Fail()
	}
	if v, err := p.SREM(key, vals...); err != nil || v != 0 {
		t.Fail()
	}
}

func TestPool_SMEMBERS_SISMEMBER(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestPool_SMEMBERS_SISMEMBER"
	if v, err := p.SMEMBERS(key); err != nil || len(v) != 0 {
		t.Fail()
	}
	if b, err := p.SISMEMBER(key, "test"); err != nil || b {
		t.Fail()
	}
	vals := make([]interface{}, 0, 20)
	m := make(map[string]int, 20)
	for i := 0; i < 20; i++ {
		v := randValue()
		vals = append(vals, v)
		m[fmt.Sprintf("%v", v)] = 1
	}
	if v, err := p.SADD(key, vals...); err != nil || v != 20 {
		t.Fail()
	}
	if v, err := p.SMEMBERS(key); err != nil || len(v) != 20 {
		t.Fail()
	} else {
		for _, val := range v {
			if _, ok := m[val]; !ok {
				t.Fail()
			}
			if b, err := p.SISMEMBER(key, val); err != nil || !b {
				t.Fail()
			}
		}
	}
}

func TestConnPool_SDIFF_SDIFFSTORE(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	val1 := []interface{}{}
	val2 := []interface{}{}
	for i := 0; i < 15; i++ {
		val1 = append(val1, i)
	}
	for i := 5; i < 15; i++ {
		val2 = append(val2, i)
	}
	key1 := "TestConnPool_SDIFF_SDIFFSTORE_1"
	key2 := "TestConnPool_SDIFF_SDIFFSTORE_2"
	if v, err := p.SDIFF(key1, key2); err != nil || len(v) != 0 {
		t.Fail()
	}
	if v, err := p.SADD(key1, val1...); err != nil || int(v) != len(val1) {
		t.Fail()
	}
	if v, err := p.SADD(key2, val2...); err != nil || int(v) != len(val2) {
		t.Fail()
	}
	if diff, err := p.SDIFF(key1, key2); err != nil || len(diff) != 5 {
		t.Log(4, diff, err)
		t.Fail()
	}

	key := "TestConnPool_SDIFF_SDIFFSTORE_S"
	if v, err := p.SDIFFSTORE(key, key1, key2); err != nil || int(v) != 5 {
		t.Fail()
	}
	if v, err := p.SCARD(key); err != nil || int(v) != 5 {
		t.Fail()
	}
}

func TestConnPool_SINTER_SINTERSTORE(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	val1 := []interface{}{}
	val2 := []interface{}{}
	for i := 0; i < 15; i++ {
		val1 = append(val1, i)
	}
	for i := 5; i < 15; i++ {
		val2 = append(val2, i)
	}
	key1 := "TestConnPool_SINTER_SINTERSTORE_1"
	key2 := "TestConnPool_SINTER_SINTERSTORE_2"
	if v, err := p.SINTER(key1, key2); err != nil || len(v) != 0 {
		t.Fail()
	}
	if v, err := p.SADD(key1, val1...); err != nil || int(v) != len(val1) {
		t.Fail()
	}
	if v, err := p.SADD(key2, val2...); err != nil || int(v) != len(val2) {
		t.Fail()
	}
	if inter, err := p.SINTER(key1, key2); err != nil || len(inter) != 10 {
		t.Fail()
	}

	key := "TestConnPool_SINTER_SINTERSTORE_S"
	if v, err := p.SINTERSTORE(key, key1, key2); err != nil || int(v) != 10 {
		t.Fail()
	}
	if v, err := p.SCARD(key); err != nil || int(v) != 10 {
		t.Fail()
	}
}

func TestConnPool_SUNION_SUNIONSTORE(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	val1 := []interface{}{}
	val2 := []interface{}{}
	for i := 0; i < 15; i++ {
		val1 = append(val1, i)
	}
	for i := 5; i < 20; i++ {
		val2 = append(val2, i)
	}
	key1 := "TestConnPool_SUNION_SUNIONSTORE_1"
	key2 := "TestConnPool_SUNION_SUNIONSTORE_2"
	if v, err := p.SUNION(key1, key2); err != nil || len(v) != 0 {
		t.Fail()
	}
	if v, err := p.SADD(key1, val1...); err != nil || int(v) != len(val1) {
		t.Fail()
	}
	if v, err := p.SADD(key2, val2...); err != nil || int(v) != len(val2) {
		t.Fail()
	}
	if union, err := p.SUNION(key1, key2); err != nil || len(union) != 20 {
		t.Fail()
	}

	key := "TestConnPool_SUNION_SUNIONSTORE_S"
	if v, err := p.SUNIONSTORE(key, key1, key2); err != nil || int(v) != 20 {
		t.Fail()
	}
	if v, err := p.SCARD(key); err != nil || int(v) != 20 {
		t.Fail()
	}
}

func TestConnPool_SMOVE(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	src := "TestConnPool_SMOVE_S"
	dest := "TestConnPool_SMOVE_D"
	val := "smove"
	if b, err := p.SMOVE(src, dest, val); err != nil || b {
		t.Fail()
	}
	if v, err := p.SADD(src, val); err != nil || int(v) != 1 {
		t.Fail()
	}
	if b, err := p.SMOVE(src, dest, val); err != nil || !b {
		t.Fail()
	}
	if v, err := p.SADD(src, val); err != nil || int(v) != 1 {
		t.Fail()
	}
	if b, err := p.SMOVE(src, dest, val); err != nil || !b {
		t.Fail()
	}
}

func TestConnPool_SPOP_SRANDMEMBER(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_SPOP_SRANDMEMBER"
	if _, err := p.SPOP(key); err != ErrNil {
		t.Fail()
	}
	if v, err := p.SPOPExtra(key, 2); err != nil || len(v) != 0 {
		t.Fail()
	}
	if v, err := p.SRANDMEMBER(key, 1); err != nil || len(v) != 0 {
		t.Fail()
	}
	vals := []interface{}{}
	for i := 0; i < 10; i++ {
		vals = append(vals, randValue())
	}
	if v, err := p.SADD(key, vals...); err != nil || v != 10 {
		t.Fail()
	}
	if v, err := p.SRANDMEMBER(key, 1); err != nil || len(v) != 1 {
		t.Fail()
	}
	if v, err := p.SRANDMEMBER(key, 10); err != nil || len(v) != 10 {
		t.Fail()
	}
	if v, err := p.SCARD(key); err != nil || v != 10 {
		t.Fail()
	}
	if v, err := p.SPOP(key); err != nil {
		t.Fail()
	} else if b, err := p.SISMEMBER(key, v); err != nil || b {
		t.Fail()
	}
	if v, err := p.SPOPExtra(key, 2); err != nil || len(v) != 2 {
		t.Fail()
	}
	if v, err := p.SCARD(key); err != nil || v != 7 {
		t.Fail()
	}
}
