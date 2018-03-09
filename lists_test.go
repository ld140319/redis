package redis

import (
	"testing"
	"time"
)

func TestConnPool_LPUSH_LPUSHX_RPOP(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_LPUSH_LPUSHX_RPOP"
	if v, err := p.LPUSHX(key, "t"); err != nil || v != 0 {
		t.Fail()
	}
	for i := 0; i < 10; i++ {
		if v, err := p.LPUSH(key, i); err != nil || int(v) != (i+1) {
			t.Fail()
		}
	}
	if v, err := p.RPOP(key); err != nil || v != "0" {
		t.Fail()
	}
}

func TestConnPool_RPUSH_RPUSHX_LPOP(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_RPUSH_RPUSHX_LPOP"
	if v, err := p.RPUSHX(key, "t"); err != nil || v != 0 {
		t.Fail()
	}
	for i := 0; i < 10; i++ {
		if v, err := p.RPUSH(key, i); err != nil || int(v) != (i+1) {
			t.Fail()
		}
	}
	if v, err := p.LPOP(key); err != nil || v != "0" {
		t.Fail()
	}
}

func TestConnPool_LPUSH_LPOP(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_LPUSH_LPOP"
	for i := 0; i < 10; i++ {
		if v, err := p.LPUSH(key, i); err != nil || int(v) != (i+1) {
			t.Fail()
		}
	}
	if v, err := p.LPOP(key); err != nil || v != "9" {
		t.Fail()
	}
}

func TestConnPool_RPUSH_RPOP(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_RPUSH_RPOP"
	for i := 0; i < 10; i++ {
		if v, err := p.RPUSH(key, i); err != nil || int(v) != (i+1) {
			t.Fail()
		}
	}
	if v, err := p.RPOP(key); err != nil || v != "9" {
		t.Fail()
	}
}

func TestConnPool_LINDEX_LINSERT_LLEN(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_LINDEX_LINSERT_LLEN"
	if v, err := p.LLEN(key); err != nil || v != 0 {
		t.Fail()
	}
	for i := 0; i < 10; i++ {
		if v, err := p.RPUSH(key, i); err != nil || int(v) != (i+1) {
			t.Fail()
		}
	}
	if v, err := p.LLEN(key); err != nil || v != 10 {
		t.Fail()
	}
	if v, err := p.LINDEX(key, 4); err != nil || v != "4" {
		t.Fail()
	}
	if v, err := p.LINSERT(key, true, "4", "t"); err != nil || v != 11 {
		t.Fail()
	}
	if v, err := p.LINDEX(key, 4); err != nil || v != "t" {
		t.Fail()
	}
	if v, err := p.LINSERT(key, false, "4", "tt"); err != nil || v != 12 {
		t.Fail()
	}
	if v, err := p.LINDEX(key, 6); err != nil || v != "tt" {
		t.Fail()
	}
}

func TestConnPool_LSET_LREM(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_LSET_LREM"
	for i := 0; i < 10; i++ {
		if v, err := p.RPUSH(key, i); err != nil || int(v) != (i+1) {
			t.Fail()
		}
	}
	if err := p.LSET(key, 4, "tt"); err != nil {
		t.Fail()
	}
	if v, err := p.LINDEX(key, 4); err != nil || v != "tt" {
		t.Fail()
	}
	if v, err := p.LREM(key, 0, "tt"); err != nil || v != 1 {
		t.Fail()
	}
}

func TestConnPool_LTRIM(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_LTRIM"
	for i := 0; i < 10; i++ {
		if v, err := p.RPUSH(key, i); err != nil || int(v) != (i+1) {
			t.Fail()
		}
	}
	if err := p.LTRIM(key, 0, 4); err != nil {
		t.Fail()
	}
	if v, err := p.LLEN(key); err != nil || v != 5 {
		t.Fail()
	}
}

func TestConnPool_LRANGE(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_LRANGE"
	if v, err := p.LRANGE(key, 0, -1); err != nil || len(v) != 0 {
		t.Fail()
	}
	for i := 0; i < 10; i++ {
		if v, err := p.RPUSH(key, i); err != nil || int(v) != (i+1) {
			t.Fail()
		}
	}
	if v, err := p.LRANGE(key, 0, -1); err != nil || len(v) != 10 {
		t.Fail()
	}
}

func TestConnPool_BLPOP_BRPOP(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_BLPOP_BRPOP"
	if _, err := p.BLPOP(1, key); err != ErrNil {
		t.Fail()
	}
	if _, err := p.BRPOP(1, key); err != ErrNil {
		t.Fail()
	}
	go func() {
		time.Sleep(time.Second)
		p.LPUSH(key, "tt")
		p.LPUSH(key, "tt")
	}()
	if v, err := p.BLPOP(2, key); err != nil || v[1] != "tt" {
		t.Fail()
	}
	if v, err := p.BRPOP(2, key); err != nil || v[1] != "tt" {
		t.Fail()
	}
}

func TestConnPool_RPOPLPUSH_BRPOPLPUSH(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	src := "TestConnPool_RPOPLPUSH_BRPOPLPUSH_S"
	dest := "TestConnPool_RPOPLPUSH_BRPOPLPUSH_D"
	if _, err := p.RPOPLPUSH(src, dest); err != ErrNil {
		t.Fail()
	}
	if v, err := p.LPUSH(src, "tt"); err != nil || v != 1 {
		t.Fail()
	}
	if v, err := p.RPOPLPUSH(src, dest); err != nil || v != "tt" {
		t.Fail()
	}
	if _, err := p.BRPOPLPUSH(src, dest, 1); err != ErrNil {
		t.Fail()
	}
	if v, err := p.BRPOPLPUSH(dest, src, 1); err != nil || v != "tt" {
		t.Fail()
	}
}
