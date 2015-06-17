// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	"math"

	"github.com/reborndb/go/redis/rdb"
	redis "github.com/reborndb/go/redis/resp"
	"github.com/reborndb/qdb/pkg/store"
)

// SELECT db
func SelectCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if db, err := store.ParseUint(args[0]); err != nil {
		return toRespError(err)
	} else if db > math.MaxUint32 {
		return toRespErrorf("parse db = %d", db)
	} else {
		c.SetDB(uint32(db))
		return redis.NewString("OK"), nil
	}
}

// DEL key [key ...]
func DelCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) == 0 {
		return toRespErrorf("len(args) = %d, expect != 1", len(args))
	}

	if n, err := c.Store().Del(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// DUMP key
func DumpCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if x, err := c.Store().Dump(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else if dump, err := rdb.EncodeDump(x); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytes(dump), nil
	}
}

// TYPE key
func TypeCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if c, err := c.Store().Type(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString(c.String()), nil
	}
}

// EXISTS key
func ExistsCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if x, err := c.Store().Exists(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// TTL key
func TTLCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if x, err := c.Store().TTL(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// PTTL key
func PTTLCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if x, err := c.Store().PTTL(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// PERSIST key
func PersistCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if x, err := c.Store().Persist(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// EXPIRE key seconds
func ExpireCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	if x, err := c.Store().Expire(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// PEXPIRE key milliseconds
func PExpireCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	if x, err := c.Store().PExpire(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// EXPIREAT key timestamp
func ExpireAtCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	if x, err := c.Store().ExpireAt(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// PEXPIREAT key timestamp
func PExpireAtCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	if x, err := c.Store().PExpireAt(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// RESTORE key ttlms value
func RestoreCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	if err := c.Store().Restore(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

func init() {
	Register("select", SelectCmd)
	Register("del", DelCmd)
	Register("dump", DumpCmd)
	Register("type", TypeCmd)
	Register("exists", ExistsCmd)
	Register("ttl", TTLCmd)
	Register("pttl", PTTLCmd)
	Register("persist", PersistCmd)
	Register("expire", ExpireCmd)
	Register("pexpire", PExpireCmd)
	Register("expireat", ExpireAtCmd)
	Register("pexpireat", PExpireAtCmd)
	Register("restore", RestoreCmd)
}
