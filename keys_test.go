package redis

import (
	"fmt"
	"testing"
	"time"
)

func TestConnPool_KEYS_EXISTS_DEL(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	akeys := []string{}
	bkeys := []interface{}{}
	kvs := make(map[string]interface{})
	for i := 0; i < 10; i++ {
		akey := fmt.Sprintf("aaa:%v", i)
		bkey := fmt.Sprintf("bbb:%v", i)
		akeys = append(akeys, akey)
		bkeys = append(bkeys, bkey)
		kvs[akey] = randValue()
		kvs[bkey] = randValue()
	}
	if v, err := p.EXISTSExtra(bkeys...); err != nil || v != 0 {
		t.Fail()
	}
	if v, err := p.DEL(bkeys...); err != nil || v != 0 {
		t.Fail()
	}
	if err := p.MSET(kvs); err != nil {
		t.Fail()
	}
	if v, err := p.KEYS("aaa*"); err != nil || len(v) != len(akeys) {
		t.Fail()
	}
	if v, err := p.KEYS("*"); err != nil || len(v) != len(akeys)+len(bkeys) {
		t.Fail()
	}
	if v, err := p.EXISTSExtra(bkeys...); err != nil || int(v) != len(bkeys) {
		t.Fail()
	}
	if v, err := p.DEL(bkeys...); err != nil || int(v) != len(bkeys) {
		t.Fail()
	}
	if v, err := p.EXISTSExtra(bkeys...); err != nil || v != 0 {
		t.Fail()
	}
}

func TestConnPool_DUMP_RESTORE_String(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_DUMP_RESTORE_String"
	value := randString(100)
	if _, err := p.DUMP(key); err != ErrNil {
		t.Fail()
	}
	if err := p.SET(key, value); err != nil {
		t.Fail()
	}
	if v, err := p.DUMP(key); err != nil || len(v) == 0 {
		t.Fail()
	} else if err = p.RESTORE(key, 0, v, false); err == nil {
		t.Fail()
	} else if err = p.RESTORE(key+"_New", 0, v, false); err != nil {
		t.Fail()
	} else if err = p.RESTORE(key, 0, v, true); err != nil {
		t.Fail()
	} else {
		if err = p.RESTORE(key, 5*1e3, v, true); err != nil {
			t.Fail()
		}
		if v, err := p.TTL(key); err != nil || v <= 0 {
			t.Fail()
		}
	}
}

func TestConnPool_EXPIRE(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_EXPIRE"
	if b, err := p.EXPIRE(key, 5); err != nil || b {
		t.Fail()
	}
	if err := p.SET(key, 1); err != nil {
		t.Fail()
	}
	if b, err := p.EXPIRE(key, 5); err != nil || !b {
		t.Fail()
	}
	if v, err := p.TTL(key); err != nil || v <= 0 {
		t.Fail()
	}
	time.Sleep(6 * time.Second)
	if _, err := p.GET(key); err != ErrNil {
		t.Fail()
	}
}

func TestConnPool_PEXPIRE(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_PEXPIRE"
	if b, err := p.PEXPIRE(key, 5*1e3); err != nil || b {
		t.Fail()
	}
	if err := p.SET(key, 1); err != nil {
		t.Fail()
	}
	if b, err := p.PEXPIRE(key, 5*1e3); err != nil || !b {
		t.Fail()
	}
	if v, err := p.PTTL(key); err != nil || v <= 0 {
		t.Fail()
	}
	time.Sleep(6 * time.Second)
	if _, err := p.GET(key); err != ErrNil {
		t.Fail()
	}
}

func TestConnPool_EXPIREAT(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_EXPIREAT"
	if b, err := p.EXPIREAT(key, time.Now().Add(time.Duration(5*time.Second)).Unix()); err != nil || b {
		t.Fail()
	}
	if err := p.SET(key, 1); err != nil {
		t.Fail()
	}
	if b, err := p.EXPIREAT(key, time.Now().Add(time.Duration(5*time.Second)).Unix()); err != nil || !b {
		t.Fail()
	}
	if v, err := p.TTL(key); err != nil || v <= 0 {
		t.Fail()
	}
	time.Sleep(6 * time.Second)
	if _, err := p.GET(key); err != ErrNil {
		t.Fail()
	}
}

func TestConnPool_PEXPIREAT(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_PEXPIREAT"
	if b, err := p.PEXPIREAT(key, time.Now().Add(time.Duration(5*time.Second)).Unix()*1e3); err != nil || b {
		t.Fail()
	}
	if err := p.SET(key, 1); err != nil {
		t.Fail()
	}
	if b, err := p.PEXPIREAT(key, time.Now().Add(time.Duration(5*time.Second)).Unix()*1e3); err != nil || !b {
		t.Fail()
	}
	if v, err := p.PTTL(key); err != nil || v <= 0 {
		t.Fail()
	}
	time.Sleep(6 * time.Second)
	if _, err := p.GET(key); err != ErrNil {
		t.Fail()
	}
}

func TestConnPool_TTL(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_TTL"
	if v, err := p.TTL(key); err != nil || v != -2 {
		t.Fail()
	}
	if err := p.SET(key, 1); err != nil {
		t.Fail()
	}
	if v, err := p.TTL(key); err != nil || v != -1 {
		t.Fail()
	}
	if b, err := p.EXPIRE(key, 5); err != nil || !b {
		t.Fail()
	}
	if v, err := p.TTL(key); err != nil || v <= 0 {
		t.Fail()
	}
	time.Sleep(6 * time.Second)
	if v, err := p.TTL(key); err != nil || v != -2 {
		t.Fail()
	}
}

func TestConnPool_PTTL(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_PTTL"
	if v, err := p.PTTL(key); err != nil || v != -2 {
		t.Fail()
	}
	if err := p.SET(key, 1); err != nil {
		t.Fail()
	}
	if v, err := p.PTTL(key); err != nil || v != -1 {
		t.Fail()
	}
	if b, err := p.PEXPIRE(key, 5*1e3); err != nil || !b {
		t.Fail()
	}
	if v, err := p.PTTL(key); err != nil || v <= 0 {
		t.Fail()
	}
	time.Sleep(6 * time.Second)
	if v, err := p.PTTL(key); err != nil || v != -2 {
		t.Fail()
	}
}

func TestConnPool_PERSIST(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_PERSIST"
	if b, err := p.PERSIST(key); err != nil || b {
		t.Fail()
	}
	if err := p.SET(key, 1); err != nil {
		t.Fail()
	}
	if b, err := p.EXPIRE(key, 5); err != nil || !b {
		t.Fail()
	}
	if v, err := p.TTL(key); err != nil || v <= 0 {
		t.Fail()
	}
	if b, err := p.PERSIST(key); err != nil || !b {
		t.Fail()
	}
	if v, err := p.TTL(key); err != nil || v != -1 {
		t.Fail()
	}
}

func TestConnPool_TYPE(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	if v, err := p.TYPE("type1"); err != nil || v != NoneType {
		t.Fail()
	}
	// string
	if err := p.SET("type2", randValue()); err != nil {
		t.Fail()
	}
	if v, err := p.TYPE("type2"); err != nil || v != StringType {
		t.Fail()
	}
	// hash
	if v, err := p.HSET("type3", randString(2), randValue()); err != nil || v != 1 {
		t.Fail()
	}
	if v, err := p.TYPE("type3"); err != nil || v != HashType {
		t.Fail()
	}
	// list
	if v, err := p.LPUSH("type4", randValue()); err != nil || v != 1 {
		t.Fail()
	}
	if v, err := p.TYPE("type4"); err != nil || v != ListType {
		t.Fail()
	}
	// set
	if v, err := p.SADD("type5", randValue()); err != nil || v != 1 {
		t.Fail()
	}
	if v, err := p.TYPE("type5"); err != nil || v != SetType {
		t.Fail()
	}
	// zset
	if v, err := p.ZADD("type6", 1, randValue()); err != nil || v != 1 {
		t.Fail()
	}
	if v, err := p.TYPE("type6"); err != nil || v != SortedSetType {
		t.Fail()
	}
}

func TestConnPool_RENAME(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	oldKey := "TestConnPool_RENAME"
	newKey := "TestConnPool_RENAME_NEW"
	if err := p.RENAME(oldKey, newKey); err == nil {
		t.Fail()
	}
	if err := p.SET(oldKey, randValue()); err != nil {
		t.Fail()
	}
	if err := p.SET(newKey, randValue()); err != nil {
		t.Fail()
	}
	if err := p.RENAME(oldKey, newKey); err != nil {
		t.Fail()
	}
	if b, err := p.EXISTS(oldKey); err != nil || b {
		t.Fail()
	}
	if b, err := p.EXISTS(newKey); err != nil || !b {
		t.Fail()
	}
}

func TestConnPool_RENAMENX(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	oldKey := "TestConnPool_RENAMENX"
	newKey := "TestConnPool_RENAMENX_NEW"
	if err := p.RENAME(oldKey, newKey); err == nil {
		t.Fail()
	}
	if err := p.SET(oldKey, randValue()); err != nil {
		t.Fail()
	}
	if err := p.SET(newKey, randValue()); err != nil {
		t.Fail()
	}
	if b, err := p.RENAMENX(oldKey, newKey); err != nil || b {
		t.Fail()
	}
	if v, err := p.DEL(newKey); err != nil || v != 1 {
		t.Fail()
	}
	if b, err := p.RENAMENX(oldKey, newKey); err != nil || !b {
		t.Fail()
	}
	if b, err := p.EXISTS(oldKey); err != nil || b {
		t.Fail()
	}
	if b, err := p.EXISTS(newKey); err != nil || !b {
		t.Fail()
	}
}

func TestConnPool_RANDOMKEY(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	if _, err := p.RANDOMKEY(); err != ErrNil {
		t.Fail()
	}
	for i := 0; i < 20; i++ {
		p.SET(randString(i+1), randValue())
	}
	if _, err := p.RANDOMKEY(); err == ErrNil {
		t.Fail()
	}
}

func TestConnPool_TOUCH(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	key := "TestConnPool_TOUCH"
	if v, err := p.TOUCH(key, "a", "b"); err != nil || v != 0 {
		t.Fail()
	}
	if err := p.SET(key, randValue()); err != nil {
		t.Fail()
	}
	if v, err := p.TOUCH(key, "a", "b"); err != nil || v != 1 {
		t.Fail()
	}
}
