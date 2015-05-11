// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	redis "github.com/reborndb/go/redis/resp"
)

// PING
func (h *Handler) Ping(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 0 {
		return toRespErrorf("len(args) = %d, expect = 0", len(args))
	}

	_, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}
	return redis.NewString("PONG"), nil
}

// ECHO text
func (h *Handler) Echo(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	_, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}
	return redis.NewString(string(args[0])), nil
}

// FLUSHALL
func (h *Handler) FlushAll(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 0 {
		return toRespErrorf("len(args) = %d, expect = 0", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if err := s.Binlog().Reset(); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

// COMPACTALL
func (h *Handler) CompactAll(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 0 {
		return toRespErrorf("len(args) = %d, expect = 0", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if err := s.Binlog().CompactAll(); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

// SHUTDOWN
func (h *Handler) Shutdown(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 0 {
		return toRespErrorf("len(args) = %d, expect = 0", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	s.Binlog().Close()
	os.Exit(0)
	return nil, nil
}

// INFO [section]
func (h *Handler) Info(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 0 && len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 0|1", len(args))
	}

	section := "all"
	if len(args) == 1 {
		section = strings.ToLower(string(args[0]))
	}

	var b bytes.Buffer

	switch section {
	case "database":
		h.infoDataBase(&b)
	case "config":
		h.infoConfig(&b)
	case "clients":
		h.infoClients(&b)
	case "replication":
		h.infoReplication(&b)
	default:
		// all
		h.infoAll(&b)
	}

	fmt.Fprintf(&b, "\r\n")

	return redis.NewBulkBytes(b.Bytes()), nil
}

func (h *Handler) infoAll(w io.Writer) {
	h.infoDataBase(w)
	fmt.Fprintf(w, "\r\n")
	h.infoConfig(w)
	fmt.Fprintf(w, "\r\n")
	h.infoClients(w)
	fmt.Fprintf(w, "\r\n")
	h.infoReplication(w)
}

func (h *Handler) infoConfig(w io.Writer) {
	fmt.Fprintf(w, "# Config\r\n")
	fmt.Fprintf(w, "%s\r\n", h.config)
}

func (h *Handler) infoDataBase(w io.Writer) {
	v, _ := h.bl.Info()

	fmt.Fprintf(w, "# Database\r\n")
	fmt.Fprintf(w, "%s\r\n", v)
}

func (h *Handler) infoClients(w io.Writer) {
	fmt.Fprintf(w, "# Clients\r\n")
	fmt.Fprintf(w, "bgsave:%d\r\n", h.counters.bgsave.Get())
	fmt.Fprintf(w, "clients:%d\r\n", h.counters.clients.Get())
	fmt.Fprintf(w, "clients_accepted:%d\r\n", h.counters.clientsAccepted.Get())
	fmt.Fprintf(w, "commands:%d\r\n", h.counters.commands.Get())
	fmt.Fprintf(w, "commands_failed:%d\r\n", h.counters.commandsFailed.Get())

}

func (h *Handler) infoReplication(w io.Writer) {
	fmt.Fprintf(w, "# Replication\r\n")

	masterAddr := h.masterAddr.Get()
	isSlave := (masterAddr != "")

	if !isSlave {
		fmt.Fprintf(w, "role:master\r\n")

		h.repl.RLock()
		defer h.repl.RUnlock()

		fmt.Fprintf(w, "master_repl_offset:%d\r\n", h.repl.masterOffset)
		if h.repl.backlogBuf == nil {
			fmt.Fprintf(w, "repl_backlog_active:0\r\n")
		} else {
			fmt.Fprintf(w, "repl_backlog_active:1\r\n")
			fmt.Fprintf(w, "repl_backlog_size:%d\r\n", h.repl.backlogBuf.Size())
			fmt.Fprintf(w, "repl_backlog_first_byte_offset:%d\r\n", h.repl.backlogOffset)
			fmt.Fprintf(w, "repl_backlog_histlen:%d\r\n", h.repl.backlogBuf.Len())
		}

		slaves := make([]string, 0, len(h.repl.slaves))
		for slave, _ := range h.repl.slaves {
			if addr := slave.nc.RemoteAddr(); addr != nil {
				slaves = append(slaves, addr.String())
			}
		}
		fmt.Fprintf(w, "slaves:%s\r\n", strings.Join(slaves, ","))
	} else {
		fmt.Fprintf(w, "role:slave\r\n")
		fmt.Fprintf(w, "sync_rdb_remains:%d\r\n", h.counters.syncRdbRemains.Get())
		fmt.Fprintf(w, "sync_cache_bytes:%d\r\n", h.counters.syncCacheBytes.Get())
		fmt.Fprintf(w, "sync_total_bytes:%d\r\n", h.counters.syncTotalBytes.Get())
		fmt.Fprintf(w, "slaveof:%s\r\n", h.masterAddr.Get())
		fmt.Fprintf(w, "slaveof_since:%d\r\n", h.syncSince.Get())
		status := "up"
		if h.masterConnState.Get() != masterConnConnected {
			status = "down"
		}
		fmt.Fprintf(w, "master_link_status:%s\r\n", status)
		// now all slaves have same priority
		fmt.Fprintf(w, "slave_priority:100\r\n")
		fmt.Fprintf(w, "slave_repl_offset:%d\r\n", h.syncOffset.Get())
	}
}

// CONFIG get key / set key value
func (h *Handler) Config(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 && len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 2 or 3", len(args))
	}

	_, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	sub, args := strings.ToLower(string(args[0])), args[1:]

	switch sub {
	default:
		return toRespErrorf("unknown sub-command = %s", sub)
	case "get":
		if len(args) != 2 {
			return toRespErrorf("len(args) = %d, expect = 2", len(args))
		}
		switch e := strings.ToLower(string(args[1])); e {
		default:
			return toRespErrorf("unknown entry %s", e)
		case "maxmemory":
			return redis.NewString("0"), nil
		}
	}
}
