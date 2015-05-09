// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	redis "github.com/reborndb/go/redis/resp"
	"github.com/reborndb/qdb/pkg/binlog"
	"github.com/reborndb/qdb/pkg/store/rocksdb"
	. "gopkg.in/check.v1"
)

var _ = Suite(&testReplSuite{})

type testReplSuite struct {
	redisExists bool

	srv1      *testReplSrvNode
	srv2      *testReplSrvNode
	redisNode *testReplRedisNode
}

func (s *testReplSuite) createServer(c *C, port int) *Handler {
	base := fmt.Sprintf("/tmp/test_qdb/service/test-replication/%d", port)
	err := os.RemoveAll(base)
	c.Assert(err, IsNil)

	err = os.Mkdir(base, 0700)
	c.Assert(err, IsNil)

	conf := rocksdb.NewDefaultConfig()
	testdb, err := rocksdb.Open(path.Join(base, "db"), conf, false)
	c.Assert(err, IsNil)

	bl := binlog.New(testdb)

	cfg := NewDefaultConfig()
	cfg.Listen = fmt.Sprintf("127.0.0.1:%d", port)
	cfg.DumpPath = path.Join(base, "rdb.dump")
	cfg.SyncFilePath = path.Join(base, "sync.pipe")

	h, err := newHandler(cfg, bl)
	c.Assert(err, IsNil)
	return h
}

func (s *testReplSuite) SetUpSuite(c *C) {
	_, err := exec.LookPath("redis-server")
	s.redisExists = (err == nil)

	redisPort := 17777
	if s.redisExists {
		s.startRedis(c, redisPort)
	}

	s.redisNode = &testReplRedisNode{redisPort}

	svr1Port := 17778
	svr2Port := 17779

	s.srv1 = &testReplSrvNode{port: svr1Port, h: s.createServer(c, svr1Port)}
	s.srv2 = &testReplSrvNode{port: svr2Port, h: s.createServer(c, svr2Port)}

	go s.srv1.h.run()
	go s.srv2.h.run()
}

func (s *testReplSuite) TearDownSuite(c *C) {
	if s.redisExists {
		s.stopRedis(c, s.redisNode.Port())
	}

	if s.srv1.h != nil {
		s.srv1.h.bl.Close()
		s.srv1.h.close()
	}

	if s.srv2.h != nil {
		s.srv2.h.bl.Close()
		s.srv2.h.close()
	}
}

type redisChecker struct {
	sync.Mutex
	ok  bool
	buf bytes.Buffer
}

func (r *redisChecker) Write(data []byte) (int, error) {
	r.Lock()
	defer r.Unlock()

	r.buf.Write(data)
	if strings.Contains(r.buf.String(), "The server is now ready to accept connections") {
		r.ok = true
	}

	return len(data), nil
}

func (s *testReplSuite) startRedis(c *C, port int) {
	checker := &redisChecker{ok: false}
	// start redis and use memory only
	cmd := exec.Command("redis-server", "--port", fmt.Sprintf("%d", port), "--save", "")
	cmd.Stdout = checker
	cmd.Stderr = checker

	err := cmd.Start()
	c.Assert(err, IsNil)

	for i := 0; i < 20; i++ {
		var ok bool
		checker.Lock()
		ok = checker.ok
		checker.Unlock()

		if ok {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	c.Fatal("redis-server can not start ok after 10s")
}

func (s *testReplSuite) stopRedis(c *C, port int) {
	cmd := exec.Command("redis-cli", "-p", fmt.Sprintf("%d", port), "shutdown", "nosave")
	cmd.Run()
}

func testDoCmd(c *C, port int, cmd string, args ...interface{}) redis.Resp {
	nc, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	c.Assert(err, IsNil)

	r := bufio.NewReaderSize(nc, 32)
	w := bufio.NewWriterSize(nc, 32)

	req := redis.NewRequest(cmd, args...)
	err = redis.Encode(w, req)
	c.Assert(err, IsNil)

	resp, err := redis.Decode(r)
	c.Assert(err, IsNil)
	return resp
}

func testDoCmdMustOK(c *C, port int, cmd string, args ...interface{}) {
	resp := testDoCmd(c, port, cmd, args...)
	c.Assert(resp, DeepEquals, redis.NewString("OK"))
	v, ok := resp.(*redis.String)
	c.Assert(ok, Equals, true)
	c.Assert(v.Value, Equals, "OK")
}

func (s *testReplSuite) doCmd(c *C, port int, cmd string, args ...interface{}) redis.Resp {
	return testDoCmd(c, port, cmd, args...)
}

func (s *testReplSuite) doCmdMustOK(c *C, port int, cmd string, args ...interface{}) {
	testDoCmdMustOK(c, port, cmd, args...)
}

type testReplConn interface {
	Close(c *C)
}

type testReplNode interface {
	Port() int
	Slaveof(c *C, port int) testReplConn
	SyncOffset(c *C) int64
}

type testReplRedisNode struct {
	port int
}

func (n *testReplRedisNode) Port() int {
	return n.port
}

type testReplRedisConn struct {
	port int
}

func (nc *testReplRedisConn) Close(c *C) {
	testDoCmd(c, nc.port, "CLIENT", "KILL", "addr", fmt.Sprintf("127.0.0.1:%d", nc.port), "type", "slave")
}

func (n *testReplRedisNode) Slaveof(c *C, port int) testReplConn {
	testDoCmdMustOK(c, n.port, "SLAVEOF", "127.0.0.1", port)

	return &testReplRedisConn{n.port}
}

func (n *testReplRedisNode) SyncOffset(c *C) int64 {
	resp := testDoCmd(c, n.port, "ROLE")

	// we only care slave replication sync offset

	rsp, ok := resp.(*redis.Array)
	c.Assert(ok, Equals, true)
	c.Assert(rsp.Value, HasLen, 5)

	offset := rsp.Value[4]

	switch t := offset.(type) {
	case *redis.Int:
		return t.Value
	case *redis.BulkBytes:
		n, err := strconv.ParseInt(string(t.Value), 10, 64)
		c.Assert(err, IsNil)
		return n
	default:
		c.Fatalf("invalid resp type %T", offset)
	}

	c.Fatal("can enter here")
	return 0
}

type testReplSrvConn struct {
	nc *conn
}

func (nc *testReplSrvConn) Close(c *C) {
	nc.nc.Close()
}

type testReplSrvNode struct {
	port int
	h    *Handler
}

func (n *testReplSrvNode) Slaveof(c *C, port int) testReplConn {
	nc, err := n.h.replicationConnectMaster(fmt.Sprintf("127.0.0.1:%d", port))
	c.Assert(err, IsNil)
	n.h.master <- nc
	return &testReplSrvConn{nc}
}

func (n *testReplSrvNode) Port() int             { return n.port }
func (n *testReplSrvNode) SyncOffset(c *C) int64 { return n.h.syncOffset }

func (s *testReplSuite) waitAndCheckSyncOffset(c *C, node testReplNode, lastSyncOffset int64) {
	for i := 0; i < 10; i++ {
		time.Sleep(500 * time.Millisecond)

		if node.SyncOffset(c) > lastSyncOffset {
			break
		}
	}
}

func (s *testReplSuite) testReplication(c *C, master testReplNode, slave testReplNode) {
	// first let both stop replication
	s.doCmdMustOK(c, master.Port(), "SLAVEOF", "NO", "ONE")
	s.doCmdMustOK(c, slave.Port(), "SLAVEOF", "NO", "ONE")

	s.doCmdMustOK(c, master.Port(), "SET", "a", "100")

	offset := int64(-1)
	// slaveof, will do full sync first, must support psync
	nc := slave.Slaveof(c, master.Port())
	defer nc.Close(c)

	s.waitAndCheckSyncOffset(c, slave, offset)

	resp := s.doCmd(c, slave.Port(), "GET", "a")
	c.Assert(slave.SyncOffset(c), Not(Equals), int64(-1))
	c.Assert(resp, DeepEquals, redis.NewBulkBytesWithString("100"))

	s.doCmdMustOK(c, master.Port(), "SET", "b", "100")

	time.Sleep(500 * time.Millisecond)
	resp = s.doCmd(c, slave.Port(), "GET", "b")
	c.Assert(resp, DeepEquals, redis.NewBulkBytesWithString("100"))

	offset = slave.SyncOffset(c)
	// now close replication connection
	nc.Close(c)
	s.doCmdMustOK(c, master.Port(), "SET", "b", "1000")

	s.waitAndCheckSyncOffset(c, slave, offset)

	resp = s.doCmd(c, slave.Port(), "GET", "b")
	c.Assert(resp, DeepEquals, redis.NewBulkBytesWithString("1000"))
}

func (s *testReplSuite) TestRedisMaster(c *C) {
	if !s.redisExists {
		c.Skip("no redis, skip")
	}
	// redis is master, and svr1 is slave
	s.testReplication(c, s.redisNode, s.srv1)
}

func (s *testReplSuite) TestRedisSlave(c *C) {
	if !s.redisExists {
		c.Skip("no redis, skip")
	}
	// redis is slave, and svr1 is master
	s.testReplication(c, s.srv1, s.redisNode)
}

func (s *testReplSuite) TestReplication(c *C) {
	// svr1 is master, svr2 is slave
	s.testReplication(c, s.srv1, s.srv2)
}
