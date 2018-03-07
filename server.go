package redis

import (
	"strconv"
	"time"

	lib "github.com/garyburd/redigo/redis"
)

type Section string

const (
	DefaultSection      Section = "default"
	Allection           Section = "all"
	ServerSection       Section = "server"
	ClientsSection      Section = "clients"
	MemorySection       Section = "memory"
	PersistenceSection  Section = "persistence"
	StatsSection        Section = "stats"
	ReplicationSection  Section = "replication"
	CpuSection          Section = "cpu"
	CommandstatsSection Section = "commandstats"
	ClusterSection      Section = "cluster"
	KeyspaceSection     Section = "keyspace"
)

// Instruct Redis to start an Append Only File rewrite process.
func (cp *ConnPool) BGREWRITEAOF() (err error) {
	conn := cp.GetMasterConn()
	_, err = conn.Do("BGREWRITEAOF")
	conn.Close()
	return
}

// Save the DB in background.
func (cp *ConnPool) BGSAVE() (err error) {
	conn := cp.GetMasterConn()
	_, err = conn.Do("BGSAVE")
	conn.Close()
	return
}

// Return the number of keys in the currently-selected database.
func (cp *ConnPool) DBSIZE() (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("DBSIZE"))
	conn.Close()
	return
}

// Delete all the keys of all the existing databases, not just the currently
// selected one. This command never fails.
func (cp *ConnPool) FLUSHALL(async bool) (err error) {
	conn := cp.GetMasterConn()
	if cp.greaterThan("4.0.0") && async {
		_, err = conn.Do("FLUSHALL", "ASYNC")
	} else {
		_, err = conn.Do("FLUSHALL")
	}
	conn.Close()
	return
}

// Delete all the keys of the currently selected DB. This command never fails.
// The time-complexity for this operation is O(N), N being the number of keys in the database.
func (cp *ConnPool) FLUSHDB(async bool) (err error) {
	conn := cp.GetMasterConn()
	if cp.greaterThan("4.0.0") && async {
		_, err = conn.Do("FLUSHDB", "ASYNC")
	} else {
		_, err = conn.Do("FLUSHDB")
	}
	conn.Close()
	return
}

// The INFO command returns information and statistics about the server in a
// format that is simple to parse by computers and easy to read by humans.
func (cp *ConnPool) INFO(section Section) (map[string]string, error) {
	conn := cp.GetMasterConn()
	v, err := lib.String(conn.Do("INFO", section))
	conn.Close()
	if err != nil {
		return nil, err
	} else {
		return parseServerResp(v), nil
	}
}

// Return the UNIX TIME of the last DB save executed with success.
func (cp *ConnPool) LASTSAVE() (v int64, err error) {
	conn := cp.GetMasterConn()
	v, err = lib.Int64(conn.Do("LASTSAVE"))
	conn.Close()
	return
}

// The SAVE commands performs a synchronous save of the dataset producing
// a point in time snapshot of all the data inside the Redis instance, in the form of an RDB file.
func (cp *ConnPool) SAVE() (err error) {
	conn := cp.GetMasterConn()
	_, err = conn.Do("SAVE")
	conn.Close()
	return
}

// Sync replication
func (cp *ConnPool) SYNC() (err error) {
	conn := cp.GetMasterConn()
	_, err = conn.Do("SYNC")
	conn.Close()
	return
}

// The TIME command returns the current server time.
//
// Time complexity: O(1)
func (cp *ConnPool) TIME() (time.Time, error) {
	conn := cp.GetMasterConn()
	v, err := lib.Strings(conn.Do("SYNC"))
	conn.Close()
	if err != nil || len(v) != 2 {
		return time.Now(), err
	}
	sec, _ := strconv.ParseInt(v[0], 10, 0)
	msec, _ := strconv.ParseInt(v[1], 10, 0)
	return time.Unix(sec, msec*1e3), nil
}
