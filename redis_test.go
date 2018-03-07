package redis

import (
	"math/rand"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func randString(length int) string {
	str := make([]byte, length)
	for i := 0; i < length; i++ {
		str[i] = byte(48 + rand.Intn(10))
	}
	return string(str)
}

func randValue() interface{} {
	switch rand.Intn(3) {
	case 0:
		return rand.Int63()
	case 1:
		return rand.Float64()
	default:
		return randString(10)
	}
}

func getDefaultPool() *ConnPool {
	mp, _ := NewMultiPool([]*Address{}, MaxConnIdle, MaxConnActive, IdleTimeout)
	cp, _ := mp.Add(NewSimpleAddr("127.0.0.1"))
	cp.FLUSHDB(false)
	return cp
}

func TestMultiPool_AddPool(t *testing.T) {
	mp, _ := NewMultiPool([]*Address{}, MaxConnIdle, MaxConnActive, IdleTimeout)
	cp, _ := mp.Add(NewSimpleAddr("127.0.0.1"))
	if cp == nil {
		t.Fail()
	}

	cp, _  = mp.Add(NewSimpleAddr("127.0.0.1:"))
	if cp == nil {
		t.Fail()
	}

	cp, _  = mp.Add(NewSimpleAddr("127.0.0.1:6379"))
	if cp == nil {
		t.Fail()
	}

	cp, _  = mp.Add(NewSimpleAddr("127.0.0.1:6379:"))
	if cp == nil {
		t.Fail()
	}

	cp, _  = mp.Add(NewSimpleAddr("127.0.0.1:6379::"))
	if cp == nil {
		t.Fail()
	}

	cp, _  = mp.Add(NewSimpleAddr("127.0.0.1:6379::1"))
	if cp == nil {
		t.Fail()
	}

	cp, _  = mp.Add(NewSimpleAddr("127.0.0.1:::"))
	if cp == nil {
		t.Fail()
	}

	cp, _  = mp.Add(NewSimpleAddr("127.0.0.1:::1"))
	if cp == nil {
		t.Fail()
	}
}
