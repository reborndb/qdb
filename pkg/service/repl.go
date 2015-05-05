// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/reborndb/go/bytesize"
	"github.com/reborndb/go/errors"
	"github.com/reborndb/go/log"
	redis "github.com/reborndb/go/redis/resp"
	"github.com/reborndb/go/ring"
	"github.com/reborndb/go/sync2"
	"github.com/reborndb/qdb/pkg/binlog"
)

func (h *Handler) initReplication(bl *binlog.Binlog) error {
	h.repl.Lock()
	defer h.repl.Unlock()

	h.repl.slaves = make(map[*conn]chan struct{})

	h.repl.lastSelectDB.Set(int64(math.MaxUint32))

	h.repl.fullSyncSema = sync2.NewSemaphore(1)

	bl.RegPostCommitHandler(h.replicationFeedSlaves)

	go func() {
		for {
			pingPeriod := time.Duration(h.config.ReplPingSlavePeriod) * time.Second
			select {
			case <-h.signal:
				return
			case <-time.After(pingPeriod):
				f := &binlog.Forward{Op: "PING",
					DB:   uint32(h.repl.lastSelectDB.Get()),
					Args: nil}
				if err := h.replicationFeedSlaves(f); err != nil {
					// ping slaves
					log.ErrorError(err, "ping slaves error")
				}
			}
		}
	}()

	return nil
}

func (h *Handler) closeReplication() error {
	h.repl.Lock()
	defer h.repl.Unlock()

	return h.destoryReplicationBacklog()
}

func (h *Handler) createReplicationBacklog() error {
	var err error
	bufSize := h.config.ReplBacklogSize

	// minimal backlog bufsize is 1MB
	if bufSize < bytesize.MB {
		bufSize = bytesize.MB
	}

	if path := h.config.ReplBacklogFilePath; len(path) == 0 {
		h.repl.backlogBuf, err = ring.NewMemRing(bufSize)
	} else {
		h.repl.backlogBuf, err = ring.NewFileRing(path, bufSize)
	}
	if err != nil {
		return errors.Trace(err)
	}

	h.repl.backlogBuf.Reset()

	// Increment the global replication offset by one to make sure
	// we will not PSYNC with any previos slave.
	h.repl.masterOffset++

	// To make sure we don't have any data in replication buffer.
	h.repl.backlogOffset = h.repl.masterOffset + 1

	return nil
}

func (h *Handler) destoryReplicationBacklog() error {
	if h.repl.backlogBuf == nil {
		return nil
	}

	err := h.repl.backlogBuf.Close()
	h.repl.backlogBuf = nil
	return errors.Trace(err)
}

func (h *Handler) feedReplicationBacklog(buf []byte) error {
	h.repl.masterOffset += int64(len(buf))

	_, err := h.repl.backlogBuf.Write(buf)
	if err != nil {
		log.ErrorError(err, "write replication backlog err, reset")
		h.destoryReplicationBacklog()
		return errors.Trace(err)
	}

	// set the offset of the first byte in the backlog
	h.repl.backlogOffset = h.repl.masterOffset - int64(h.repl.backlogBuf.Len()) + 1

	return nil
}

func respEncodeBinlogForward(f *binlog.Forward) ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("*%d\r\n", len(f.Args)+1))
	buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(f.Op), f.Op))

	for _, arg := range f.Args {
		switch t := arg.(type) {
		case []byte:
			buf.WriteString(fmt.Sprintf("$%d\r\n", len(t)))
			buf.Write(t)
			buf.WriteString("\r\n")
		case string:
			buf.WriteString(fmt.Sprintf("$%d\r\n", len(t)))
			buf.WriteString(t)
			buf.WriteString("\r\n")
		default:
			str := fmt.Sprintf("%v", t)
			buf.WriteString(fmt.Sprintf("$%d\r\n", len(str)))
			buf.WriteString(str)
			buf.WriteString("\r\n")
		}
	}
	return buf.Bytes(), nil
}

func (h *Handler) replicationFeedSlaves(f *binlog.Forward) error {
	h.repl.Lock()
	defer h.repl.Unlock()

	r := &h.repl
	if r.backlogBuf == nil && len(r.slaves) == 0 {
		return nil
	}

	if r.backlogBuf == nil {
		if err := h.createReplicationBacklog(); err != nil {
			return errors.Trace(err)
		}
	}

	if r.lastSelectDB.Get() != int64(f.DB) {
		dbStr := fmt.Sprintf("%d", f.DB)
		selectCmd := fmt.Sprintf("*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n", len(dbStr), dbStr)

		// write SELECT into backlog
		if err := h.feedReplicationBacklog([]byte(selectCmd)); err != nil {
			return errors.Trace(err)
		}

		r.lastSelectDB.Set(int64(f.DB))
	}

	// encode Forward with RESP format, then write into backlog
	if buf, err := respEncodeBinlogForward(f); err != nil {
		return errors.Trace(err)
	} else if err = h.feedReplicationBacklog(buf); err != nil {
		return errors.Trace(err)
	}

	// notice slaves replication backlog has new data, need to sync
	if err := h.replicationNoticeSlavesSyncing(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (h *Handler) replicationNoticeSlavesSyncing() error {
	for _, ch := range h.repl.slaves {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	return nil
}

func checkConn(arg0 interface{}, args [][]byte) (*conn, error) {
	s, _ := arg0.(*conn)
	if s == nil {
		return nil, errors.New("invalid connection")
	}
	for i, v := range args {
		if len(v) == 0 {
			return nil, errors.Errorf("args[%d] is nil", i)
		}
	}
	return s, nil
}

func (h *Handler) ReplConf(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	c, err := checkConn(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	switch strings.ToLower(string(args[0])) {
	case "listening-port":
		if port, err := strconv.ParseInt(string(args[1]), 10, 16); err != nil {
			return toRespErrorf("invalid port REPLCONF listening-port, err: %v", err)
		} else {
			c.listeningPort.Set(int64(port))
		}
	case "ack":
		if ack, err := strconv.ParseInt(string(args[1]), 10, 64); err != nil {
			return toRespErrorf("invalid port REPLCONF ACK, err: %v", err)
		} else {
			c.backlogACKOffset.Set(ack)
			c.backlogACKTime.Set(time.Now().Unix())
			// ACK will not reply anything
			return nil, nil
		}
	default:
		return toRespErrorf("Unrecognized REPLCONF option:%s", args[0])
	}

	return redis.NewString("OK"), nil
}

func (h *Handler) Sync(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	return h.handleSyncCommand("sync", arg0, args)
}

func (h *Handler) PSync(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}
	return h.handleSyncCommand("psync", arg0, args)
}

func (h *Handler) handleSyncCommand(opt string, arg0 interface{}, args [][]byte) (redis.Resp, error) {
	// check args here
	c, err := checkConn(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if h.isSlave(c) {
		// ignore SYNC if already slave
		return nil, nil
	}

	if opt == "psync" {
		// not supported now
		return toRespErrorf("psync is not supported now")
	}

	if ok := h.repl.fullSyncSema.AcquireTimeout(time.Second); !ok {
		return toRespErrorf("wait other slave full sync bgsave timeout")
	}
	defer h.repl.fullSyncSema.Release()

	// now begin full sync
	h.counters.syncFull.Add(1)

	rdb, offset, err := h.replicationBgSave(c.Binlog())
	if err != nil {
		return toRespError(err)
	}
	defer rdb.Close()

	// send rdb to slave
	st, err := rdb.Stat()
	if err != nil {
		return toRespErrorf("get rdb stat err %v", err)
	}

	rdbSize := st.Size()

	if err = c.writeRDBFrom(rdbSize, rdb); err != nil {
		// close this connection here???
		log.ErrorErrorf(err, "slave %s sync rdb err", c.summ)
		c.Close()
		return nil, err
	}

	c.syncOffset.Set(offset)

	// we may not receive any data, so ignore timeout
	c.timeout = 0

	h.startSlaveReplication(c)

	return nil, nil
}

func (h *Handler) startSlaveReplication(c *conn) {
	h.repl.Lock()
	defer h.repl.Unlock()

	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	h.repl.slaves[c] = ch

	go func(c *conn, ch chan struct{}) {
		defer func() {
			h.removeSlave(c)
			c.Close()
		}()

		buf := make([]byte, bytesize.MB)

		for {
			select {
			case <-h.signal:
				return
			case _, ok := <-ch:
				if !ok {
					return
				}

				for {
					n, err := h.replicationSlaveSyncBacklog(c, buf)
					if err != nil {
						log.ErrorErrorf(err, "sync slave err, close replication")
						return
					} else if n < len(buf) {
						// we now sync all backlog, wait new incoming
						break
					}
				}

			}
		}
	}(c, ch)
}

func (h *Handler) replicationSlaveSyncBacklog(c *conn, buf []byte) (int, error) {
	h.repl.RLock()
	defer h.repl.RUnlock()

	offset := c.syncOffset.Get()

	r := &h.repl

	if r.backlogBuf == nil {
		return 0, nil
	}

	start := r.backlogOffset
	end := r.backlogOffset + int64(r.backlogBuf.Len())

	if offset < start || offset > end {
		// we can not read data from this offset in backlog buffer
		// lag behind too much, so a better way is to stop the replication and re-fullsync again

		return 0, fmt.Errorf("slave %s has invalid sync offset %d, not in [%d, %d]", c.summ, offset, start, end)
	}

	// read data into buf
	n, err := h.repl.backlogBuf.ReadAt(buf, offset-start)
	if err != nil {
		return 0, fmt.Errorf("slave %s read backlog data err %v", c.summ, err)
	}

	if n == 0 {
		// no more data to read
		return 0, nil
	}

	// use write timeout here, now 5s
	c.nc.SetWriteDeadline(time.Now().Add(5 * time.Second))

	if err = c.writeRaw(buf[0:n]); err != nil {
		return 0, fmt.Errorf("slave %s sync backlog data err %v", c.summ, err)
	}

	c.syncOffset.Add(int64(n))

	return n, nil
}

func (h *Handler) isSlave(c *conn) bool {
	h.repl.Lock()
	defer h.repl.Unlock()

	_, ok := h.repl.slaves[c]
	return ok
}

func (h *Handler) removeSlave(c *conn) {
	h.repl.Lock()
	defer h.repl.Unlock()

	ch, ok := h.repl.slaves[c]
	if ok {
		delete(h.repl.slaves, c)
		close(ch)
	}
}

func (h *Handler) removeAllSlaves() {
	h.repl.Lock()
	defer h.repl.Unlock()

	for c, ch := range h.repl.slaves {
		delete(h.repl.slaves, c)
		close(ch)
	}
}

func (h *Handler) replicationBgSave(bl *binlog.Binlog) (*os.File, int64, error) {
	// need to improve later

	bg := h.counters.bgsave.Add(1)
	defer h.counters.bgsave.Sub(1)

	if bg != 1 {
		// unlike Redis, we don't wait bgsave now
		// will improve laster
		return nil, 0, fmt.Errorf("bgsave is busy: %d, should be 1", bg)
	}

	syncOffset := new(int64)
	sp, err := bl.NewSnapshotFunc(func() {
		offset := h.repl.masterOffset
		// we will sync from masterOffset + 1
		*syncOffset = offset + 1
		if h.repl.backlogBuf == nil {
			// we will create backlog buffer and increment master offset by one later
			*syncOffset = offset + 2
		}

		h.repl.lastSelectDB.Set(int64(math.MaxUint32))
	})
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	defer bl.ReleaseSnapshot(sp)

	path := h.config.DumpPath
	if err := h.bgsaveTo(sp, path); err != nil {
		return nil, 0, errors.Trace(err)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	return f, *syncOffset, nil
}
