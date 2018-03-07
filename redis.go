package redis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lib "github.com/garyburd/redigo/redis"
	"github.com/mcuadros/go-version"
)

const (
	MaxConnIdle   = 60
	MaxConnActive = 120
	IdleTimeout   = 20 * time.Second
)

var (
	ErrNil        = errors.New("redis nil")
	ErrNotSupport = errors.New("redis version not support")
)

// New redis connection string
// Format: host:[[port]:[password]:[db]
// Example:
// "127.0.0.1", "127.0.0.1:", "127.0.0.1:6379", "127.0.0.1:6379:", "127.0.0.1:6379::", "127.0.0.1:6379::1"
func NewConnString(host string, port int, password string, db int) string {
	if port <= 0 {
		port = 6379
	}
	if db < 0 || db > 9 {
		db = 0
	}
	return fmt.Sprintf("%v:%v:%v:%v", host, port, password, db)
}

type connAddr struct {
	Host     string
	Password string
	Database int
}

// Parse redis address from connection string
func parseAddr(addr string) *connAddr {
	parts := strings.Split(addr, ":")
	host := parts[0]
	if len(parts) > 1 && parts[1] != "" {
		host += ":" + parts[1]
	} else {
		host += ":6379"
	}
	pwd := ""
	if len(parts) > 2 {
		pwd = parts[2]
	}
	db := 0
	if len(parts) > 3 {
		db, _ = strconv.Atoi(parts[3])
	}
	return &connAddr{
		Host:     host,
		Password: pwd,
		Database: db,
	}
}

func parseServerResp(v string) map[string]string {
	lines := strings.Split(v, "\r\n")
	ret := make(map[string]string, len(lines))
	for _, line := range lines {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		kv := strings.Split(line, ":")
		if len(kv) != 2 {
			continue
		}
		ret[kv[0]] = kv[1]
	}
	return ret
}

type connArgs struct {
	MaxIdle     int
	MaxActive   int
	IdleTimeout time.Duration
	WaitIdle    bool
}

type Address struct {
	Master string   `json:"master"`
	Slaves []string `json:"slaves"`
}

func NewSimpleAddr(addr string) *Address {
	return &Address{
		Master: addr,
	}
}

type pool struct {
	*lib.Pool
	Addr *connAddr
	Args *connArgs
}

func newPool(addr *connAddr, args *connArgs) *pool {
	return &pool{
		Addr: addr,
		Args: args,
		Pool: &lib.Pool{
			MaxIdle:     args.MaxIdle,
			MaxActive:   args.MaxActive,
			IdleTimeout: time.Duration(args.IdleTimeout) * time.Second,
			Wait:        args.WaitIdle,
			Dial: func() (lib.Conn, error) {
				c, err := lib.Dial("tcp", addr.Host)
				if err != nil {
					return nil, err
				}
				if addr.Password != "" {
					if _, err := c.Do("AUTH", addr.Password); err != nil {
						c.Close()
						return nil, err
					}
				}
				return c, err
			},
			TestOnBorrow: func(c lib.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		},
	}
}

// Get connection from Pool
func (p *pool) getConn() lib.Conn {
	conn := p.Get()
	if db := p.Addr.Database; db != 0 {
		// ignore error
		conn.Do("SELECT", db)
	}
	return conn
}

// Available since 2.8.12.
func (p *pool) ROLE() (error) {
	conn := p.getConn()
	conn.Do("ROLE")
	conn.Close()
	// todo
	return nil
}

type ConnPool struct {
	master  *pool
	version string

	slaves []*pool
	idx    uint64
}

func NewConnPool(m string, ss []string, args *connArgs) (*ConnPool, error) {
	cp := &ConnPool{}
	// new Pool for master and get master version
	addr := parseAddr(m)
	cp.master = newPool(addr, args)
	if info, err := cp.INFO(ServerSection); err != nil {
		return nil, err
	} else {
		cp.version = info["redis_version"]
		// minimum redis version is 2.6.12
		if cp.lessThan("2.6.12") {
			return nil, ErrNotSupport
		}
	}
	// new Pool for slaves
	for _, s := range ss {
		addr := parseAddr(s)
		if slave := newPool(addr, args); slave != nil {
			cp.slaves = append(cp.slaves, slave)
		}
	}
	return cp, nil
}

func (cp *ConnPool) greaterThan(v string) bool {
	return version.CompareSimple(cp.version, v) > 0
}

func (cp *ConnPool) lessThan(v string) bool {
	return version.Compare(cp.version, v, "<")
}

// Get master redis server version
func (cp *ConnPool) GetVersion() string {
	return cp.version
}

// Get redis master connection from ConnPool
func (cp *ConnPool) GetMasterConn() lib.Conn {
	return cp.master.getConn()
}

// Get redis slave connection from ConnPool
func (cp *ConnPool) GetSlaveConn() lib.Conn {
	if len(cp.slaves) == 0 {
		return cp.GetMasterConn()
	}
	n := atomic.AddUint64(&cp.idx, 1)
	n = n % uint64(len(cp.slaves))
	slave := cp.slaves[int(n)]
	return slave.getConn()
}

type MultiPool struct {
	sync.RWMutex
	pools map[string]*ConnPool
	args  *connArgs
}

func NewMultiPool(addrs []*Address, maxIdle, maxActive int, idleTimeout time.Duration) (*MultiPool, error) {
	mp := &MultiPool{
		pools: make(map[string]*ConnPool),
		args: &connArgs{
			MaxIdle:     maxIdle,
			MaxActive:   maxActive,
			IdleTimeout: idleTimeout,
			WaitIdle:    true,
		},
	}
	for _, addr := range addrs {
		if _, err := mp.Add(addr); err != nil {
			return nil, err
		}
	}
	return mp, nil
}

// New ConnPool and add it to MultiPool
func (mp *MultiPool) Add(addr *Address) (*ConnPool, error) {
	cp, err := NewConnPool(addr.Master, addr.Slaves, mp.args)
	if err != nil {
		return nil, err
	}
	mp.Lock()
	mp.pools[addr.Master] = cp
	mp.Unlock()
	return cp, nil
}

// Get ConnPool by master connection string
func (mp *MultiPool) Get(master string) *ConnPool {
	mp.RLock()
	cp, ok := mp.pools[master]
	mp.RUnlock()
	if ok {
		return cp
	} else {
		cp, _ := mp.Add(NewSimpleAddr(master))
		return cp
	}
}
