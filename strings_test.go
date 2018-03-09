package redis

import (
	"testing"
)

func TestConnPool_APPEND(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_APPEND"
	val := randString(10)
	if v, err := p.APPEND(key, val); err != nil || int(v) != len(val) {
		t.Fail()
	}
	if v, err := p.APPEND(key, val); err != nil || int(v) != len(val)*2 {
		t.Fail()
	}
}

func TestConnPool_DECR_DECRBY(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_DECR_DECRBY"
	if v, err := p.DECR(key); err != nil || v != -1 {
		t.Fail()
	}
	if v, err := p.DECRBY(key, 9); err != nil || v != -10 {
		t.Fail()
	}
	if v, err := p.GET(key); err != nil || v != "-10" {
		t.Fail()
	}
}

func TestConnPool_INCR_INCRBY_INCRBYFLOAT(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_INCR_INCRBY_INCRBYFLOAT"
	if v, err := p.INCR(key); err != nil || v != 1 {
		t.Fail()
	}
	if v, err := p.INCRBY(key, 9); err != nil || v != 10 {
		t.Fail()
	}
	if v, err := p.GET(key); err != nil || v != "10" {
		t.Fail()
	}
	if v, err := p.INCRBYFLOAT(key, 10.01); err != nil || v != 20.01 {
		t.Fail()
	}
	if v, err := p.GET(key); err != nil || v != "20.01" {
		t.Fail()
	}
}

func TestConnPool_SET_GET_GETSET(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_SET_GET_GETSET"
	if _, err := p.GET(key); err != ErrNil {
		t.Fail()
	}
	val := randString(10)
	if _, err := p.GETSET(key, val); err != ErrNil {
		t.Fail()
	}
	val = randString(11)
	if err := p.SET(key, val); err != nil {
		t.Fail()
	}
	if v, err := p.GET(key); err != nil || v != val {
		t.Fail()
	}
	if v, err := p.GETSET(key, randValue()); err != nil || v != val {
		t.Fail()
	}
	if err := p.SETExtra(key, val, 0, 0, AddOnly); err != ErrNil {
		t.Fail()
	}
	if err := p.SETExtra(key, val, 10, 10000, UpdateOnly); err != nil {
		t.Fail()
	}
	if v, err := p.TTL(key); err != nil || v <= 0 {
		t.Fail()
	}
	if err := p.SETExtra(key+"_N", val, 10, 0, UpdateOnly); err != ErrNil {
		t.Fail()
	}
	if err := p.SETExtra(key+"_N", val, 10, 0, AddOnly); err != nil {
		t.Fail()
	}
	if v, err := p.TTL(key + "_N"); err != nil || v <= 0 {
		t.Fail()
	}
}

func TestConnPool_SETEX_PSETEX_SETNX(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_SETEX_PSETEX_SETNX"
	if v, err := p.SETNX(key, randValue()); err != nil || !v {
		t.Fail()
	}
	if err := p.SETEX(key, 10, randValue()); err != nil {
		t.Fail()
	}
	if v, err := p.TTL(key); err != nil || v <= 0 {
		t.Fail()
	}
	if err := p.PSETEX(key, 10*1e3, randValue()); err != nil {
		t.Fail()
	}
	if v, err := p.TTL(key); err != nil || v <= 0 {
		t.Fail()
	}
	if v, err := p.SETNX(key, randValue()); err != nil || v {
		t.Fail()
	}
}

func TestConnPool_GETRANGE_SETRANGE_STRLEN(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_GETRANGE_SETRANGE_STRLEN"
	val := randString(30)
	if v, err := p.GETRANGE(key, 0, 10); err != nil || len(v) != 0 {
		t.Fail()
	}
	if v, err := p.STRLEN(key); err != nil || v != 0 {
		t.Fail()
	}
	if v, err := p.SETRANGE(key, 0, val); err != nil || int(v) != len(val) {
		t.Fail()
	}
	if v, err := p.GETRANGE(key, 0, len(val)); err != nil || v != val {
		t.Fail()
	}
	if v, err := p.SETRANGE(key, 50, val); err != nil || int(v) != len(val)+50 {
		t.Fail()
	}
}

func TestConnPool_MSET_MSETNX_MGET(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_MSET_MSETNX_MGET"
	m := make(map[string]interface{}, 21)
	m[key] = randValue()
	for i := 0; i < 20; i++ {
		m["key_"+randString(i+1)] = randValue()
	}
	if _, err := p.MGET("key1", "key2", "key1"); err != nil {
		t.Fail()
	}
	if err := p.SET(key, randValue()); err != nil {
		t.Fail()
	}
	if b, err := p.MSETNX(m); err != nil || b {
		t.Fail()
	}
	if err := p.MSET(m); err != nil {
		t.Fail()
	}
}

func TestConnPool_SETBIT_GETBIT(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_SETBIT_GETBIT"
	if v, err := p.GETBIT(key, 1); err != nil || v {
		t.Fail()
	}
	if v, err := p.SETBIT(key, 1, true); err != nil || v {
		t.Fail()
	}
	if v, err := p.GETBIT(key, 1); err != nil || !v {
		t.Fail()
	}
}

func TestConnPool_BITCOUNT(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_BITCOUNT"
	for i := 0; i < 10; i++ {
		if v, err := p.SETBIT(key, i, true); err != nil || v {
			t.Fail()
		}
		if v, err := p.BITCOUNT(key, 0, -1); err != nil || int(v) != (i+1) {
			t.Fail()
		}
	}
	if v, err := p.BITCOUNT(key, 0, 0); err != nil || int(v) != 8 {
		t.Fail()
	}
	if v, err := p.BITCOUNT(key, 1, 1); err != nil || int(v) != 2 {
		t.Fail()
	}
}

func TestConnPool_BITPOS(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_BITPOS"
	if v, err := p.BITPOS(key, true, 0, -1); err != nil || v != -1 {
		t.Fail()
	}
	if v, err := p.SETBIT(key, 10, true); err != nil || v {
		t.Fail()
	}
	if v, err := p.BITPOS(key, true, 0, -1); err != nil || v != 10 {
		t.Fail()
	}
}

func TestConnPool_BITOP(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	dest := "TestConnPool_BITOP"
	key1 := "TestConnPool_BITOP_1"
	key2 := "TestConnPool_BITOP_2"
	if err := p.SET(key1, []byte{0x4f}); err != nil {
		t.Fail()
	}
	if err := p.SET(key2, []byte{0xf1}); err != nil {
		t.Fail()
	}
	if _, err := p.BITOP(BitAnd, dest, key1, key2); err != nil {
		t.Fail()
	} else if v, err := p.GET(dest); err != nil || v != "A" {
		t.Fail()
	}
	if err := p.SET(key1, []byte{0x40}); err != nil {
		t.Fail()
	}
	if err := p.SET(key2, []byte{0x2}); err != nil {
		t.Fail()
	}
	if _, err := p.BITOP(BitOr, dest, key1, key2); err != nil {
		t.Fail()
	} else if v, err := p.GET(dest); err != nil || v != "B" {
		t.Fail()
	}
	if err := p.SET(key1, []byte{0xbc}); err != nil {
		t.Fail()
	}
	if err := p.SET(key2, []byte{0xff}); err != nil {
		t.Fail()
	}
	if _, err := p.BITOP(BitXor, dest, key1, key2); err != nil {
		t.Fail()
	} else if v, err := p.GET(dest); err != nil || v != "C" {
		t.Fail()
	}
	if err := p.SET(key1, []byte{0xbb}); err != nil {
		t.Fail()
	}
	if _, err := p.BITOP(BitNot, dest, key1); err != nil {
		t.Fail()
	} else if v, err := p.GET(dest); err != nil || v != "D" {
		t.Fail()
	}
}
