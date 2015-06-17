// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	redis "github.com/reborndb/go/redis/resp"
	"github.com/reborndb/qdb/pkg/store"
)

// HGETALL key
func HGetAllCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if a, err := c.Store().HGetAll(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range a {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// HDEL key field [field ...]
func HDelCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) < 2 {
		return toRespErrorf("len(args) = %d, expect >= 2", len(args))
	}

	if n, err := c.Store().HDel(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// HEXISTS key field
func HExistsCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	if x, err := c.Store().HExists(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// HGET key field
func HGetCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	if b, err := c.Store().HGet(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytes(b), nil
	}
}

// HLEN key
func HLenCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if n, err := c.Store().HLen(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// HINCRBY key field delta
func HIncrByCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	if v, err := c.Store().HIncrBy(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// HINCRBYFLOAT key field delta
func HIncrByFloatCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	if v, err := c.Store().HIncrByFloat(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytesWithString(store.FormatFloatString(v)), nil
	}
}

// HKEYS key
func HKeysCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if a, err := c.Store().HKeys(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range a {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// HVALS key
func HValsCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if a, err := c.Store().HVals(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range a {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// HSET key field value
func HSetCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	if x, err := c.Store().HSet(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// HSETNX key field value
func HSetNXCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	if x, err := c.Store().HSetNX(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// HMSET key field value [field value ...]
func HMSetCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) == 1 || len(args)%2 != 1 {
		return toRespErrorf("len(args) = %d, expect != 1 && mod 2 = 1", len(args))
	}

	if err := c.Store().HMSet(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

// HMGET key field [field ...]
func HMGetCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) < 2 {
		return toRespErrorf("len(args) = %d, expect >= 2", len(args))
	}

	if a, err := c.Store().HMGet(c.DB(), iconvert(args)...); err != nil {
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
	Register("hgetall", HGetAllCmd)
	Register("hdel", HDelCmd)
	Register("hexists", HExistsCmd)
	Register("hget", HGetCmd)
	Register("hlen", HLenCmd)
	Register("hincrby", HIncrByCmd)
	Register("hincrbyfloat", HIncrByFloatCmd)
	Register("hkeys", HKeysCmd)
	Register("hvals", HValsCmd)
	Register("hset", HSetCmd)
	Register("hsetnx", HSetNXCmd)
	Register("hmset", HMSetCmd)
	Register("hmget", HMGetCmd)
}
