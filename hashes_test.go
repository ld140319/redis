package redis

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestConnPool_HSET_HEXIST_HGET_HDEL(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_HSET_HEXIST_HGET_HDEL"
	field := "test_1"
	value := randValue()

	if _, err := p.HGET(key, field); err != ErrNil {
		t.Fail()
	}
	if b, err := p.HEXISTS(key, field); err != nil || b {
		t.Fail()
	}
	if v, err := p.HDEL(key, field); err != nil || v != 0 {
		t.Fail()
	}

	if v, err := p.HSET(key, field, value); err != nil || v != 1 {
		t.Fail()
	}
	if b, err := p.HEXISTS(key, field); err != nil || !b {
		t.Fail()
	}
	if _, err := p.HGET(key, field+"xxx"); err != ErrNil {
		t.Fail()
	}
	if v, err := p.HGET(key, field); err != nil || fmt.Sprintf("%v", value) != v {
		t.Fail()
	}
	if v, err := p.HDEL(key, field); err != nil || v != 1 {
		t.Fail()
	}
	if _, err := p.HGET(key, field); err != ErrNil {
		t.Fail()
	}
	if b, err := p.HEXISTS(key, field); err != nil || b {
		t.Fail()
	}
}

func TestConnPool_HKEYS_HVALS_HLEN(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_HKEYS_HVALS_HLEN"

	if v, err := p.HKEYS(key); err != nil || len(v) != 0 {
		t.Fail()
	}
	if v, err := p.HVALS(key); err != nil || len(v) != 0 {
		t.Fail()
	}
	if v, err := p.HLEN(key); err != nil || v != 0 {
		t.Fail()
	}

	for i := 0; i < 20; i++ {
		p.HSET(key, randString(i+1), randValue())
	}
	if v, err := p.HKEYS(key); err != nil || len(v) != 20 {
		t.Fail()
	}
	if v, err := p.HVALS(key); err != nil || len(v) != 20 {
		t.Fail()
	}
	if v, err := p.HLEN(key); err != nil || v != 20 {
		t.Fail()
	}
}

func TestConnPool_HSETNX(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_HSETNX"
	field := "nx"

	if _, err := p.HGET(key, field); err != ErrNil {
		t.Fail()
	}
	if v, err := p.HSETNX(key, field, randValue()); err != nil || !v {
		t.Fail()
	}
	if v, err := p.HSETNX(key, field, randValue()); err != nil || v {
		t.Fail()
	}
}

func TestConnPool_HMSET_HMGET_HGETALL(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_HMSET_HMGET_HGETALL"
	if v, err := p.HMGET(key, "t1", "t2"); err != nil {
		t.Fail()
	} else if v["t1"] != "" || v["t2"] != "" {
		t.Fail()
	}
	if v, err := p.HGETALL(key); err != nil || len(v) != 0 {
		t.Fail()
	}

	m := make(map[string]interface{})
	for i := 0; i < 20; i++ {
		m[randString(i+1)] = randValue()
	}

	if err := p.HMSET(key, m); err != nil {
		t.Fail()
	}
	if r, err := p.HGETALL(key); err != nil {
		t.Fail()
	} else {
		for k, v := range r {
			if orgin, ok := m[k]; !ok || fmt.Sprintf("%v", orgin) != v {
				t.Fail()
			}
		}
	}

	ks := make([]interface{}, 0, len(m))
	for k, _ := range m {
		ks = append(ks, k)
	}
	if r, err := p.HMGET(key, ks...); err != nil {
		t.Fail()
	} else {
		for k, v := range r {
			if orgin, ok := m[k]; !ok || fmt.Sprintf("%v", orgin) != v {
				t.Fail()
			}
		}
	}
}

func TestConnPool_HSTRLEN(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_HSTRLEN"
	field := "strlen"
	value := randString(rand.Intn(30))

	if v, err := p.HSTRLEN(key, field); err != nil || v != 0 {
		t.Fail()
	}
	if v, err := p.HSETNX(key, field, value); err != nil || !v {
		t.Fail()
	}
	if v, err := p.HSTRLEN(key, field); err != nil || int(v) != len(value) {
		t.Fail()
	}
}

func TestConnPool_HINCRBY_HINCRBYFLOAT(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_HINCRBY_HINCRBYFLOAT"

	if _, err := p.HGET(key, "intval"); err != ErrNil {
		t.Fail()
	}
	if _, err := p.HGET(key, "floatval"); err != ErrNil {
		t.Fail()
	}
	for i := 0; i < 10; i++ {
		if v, err := p.HINCRBY(key, "intval", 1); err != nil || int(v) != i+1 {
			t.Fail()
		}
		if v, err := p.HINCRBYFLOAT(key, "floatval", 1.2); err != nil {
			t.Fail()
		} else if math.Abs(v-float64(i+1)*1.2) > 0.0000001 {
			t.Fail()
		}
	}
}

func TestConnPool_HSCAN(t *testing.T) {

}
