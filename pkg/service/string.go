// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	redis "github.com/reborndb/go/redis/resp"
	"github.com/reborndb/qdb/pkg/store"
)

// GET key
func GetCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if b, err := c.Store().Get(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytes(b), nil
	}
}

// APPEND key value
func AppendCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	if n, err := c.Store().Append(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// SET key value
func SetCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	if err := c.Store().Set(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

// PSETEX key milliseconds value
func PSetEXCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	if err := c.Store().PSetEX(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

// SETEX key seconds value
func SetEXCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	if err := c.Store().SetEX(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

// SETNX key value
func SetNXCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	if n, err := c.Store().SetNX(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// GETSET key value
func GetSetCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	if b, err := c.Store().GetSet(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytes(b), nil
	}
}

// INCR key
func IncrCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if v, err := c.Store().Incr(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// INCRBY key delta
func IncrByCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	if v, err := c.Store().IncrBy(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// DECR key
func DecrCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if v, err := c.Store().Decr(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// DECRBY key delta
func DecrByCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	if v, err := c.Store().DecrBy(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// INCRBYFLOAT key delta
func IncrByFloatCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	if v, err := c.Store().IncrByFloat(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytesWithString(store.FormatFloatString(v)), nil
	}
}

// SETBIT key offset value
func SetBitCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	if x, err := c.Store().SetBit(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// SETRANGE key offset value
func SetRangeCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	if x, err := c.Store().SetRange(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// MSET key value [key value ...]
func MSetCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return toRespErrorf("len(args) = %d, expect != 0 && mod 2 = 0", len(args))
	}

	if err := c.Store().MSet(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

// MSETNX key value [key value ...]
func MSetNXCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return toRespErrorf("len(args) = %d, expect != 0 && mod 2 = 0", len(args))
	}

	if n, err := c.Store().MSetNX(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// MGET key [key ...]
func MGetCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) < 1 {
		return toRespErrorf("len(args) = %d, expect >= 1", len(args))
	}

	if a, err := c.Store().MGet(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range a {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

func init() {
	Register("get", GetCmd)
	Register("append", AppendCmd)
	Register("set", SetCmd)
	Register("psetex", PSetEXCmd)
	Register("setex", SetEXCmd)
	Register("setnx", SetNXCmd)
	Register("getset", GetSetCmd)
	Register("incr", IncrCmd)
	Register("incrby", IncrByCmd)
	Register("decr", DecrCmd)
	Register("decrby", DecrByCmd)
	Register("incrbyfloat", IncrByFloatCmd)
	Register("setbit", SetBitCmd)
	Register("setrange", SetRangeCmd)
	Register("mset", MSetCmd)
	Register("msetnx", MSetNXCmd)
	Register("mget", MGetCmd)
}
