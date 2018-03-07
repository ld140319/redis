package redis

import (
	"testing"
)

func TestConnPool_RANDOMKEY(t *testing.T) {
	p := getDefaultPool()
	if p == nil {
		t.Fail()
	}
	p.GetMasterConn().Do("FLUSHALL")
	if _, err := p.RANDOMKEY(); err != ErrNil {
		t.Fail()
	}
}
