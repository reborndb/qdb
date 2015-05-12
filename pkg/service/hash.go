// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	redis "github.com/reborndb/go/redis/resp"
	"github.com/reborndb/qdb/pkg/store"
)

// HGETALL key
func (h *Handler) HGetAll(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if a, err := s.Store().HGetAll(s.DB(), iconvert(args)...); err != nil {
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
func (h *Handler) HDel(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) < 2 {
		return toRespErrorf("len(args) = %d, expect >= 2", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if n, err := s.Store().HDel(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// HEXISTS key field
func (h *Handler) HExists(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if x, err := s.Store().HExists(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// HGET key field
func (h *Handler) HGet(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if b, err := s.Store().HGet(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytes(b), nil
	}
}

// HLEN key
func (h *Handler) HLen(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if n, err := s.Store().HLen(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// HINCRBY key field delta
func (h *Handler) HIncrBy(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if v, err := s.Store().HIncrBy(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// HINCRBYFLOAT key field delta
func (h *Handler) HIncrByFloat(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if v, err := s.Store().HIncrByFloat(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString(store.FormatFloatString(v)), nil
	}
}

// HKEYS key
func (h *Handler) HKeys(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if a, err := s.Store().HKeys(s.DB(), iconvert(args)...); err != nil {
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
func (h *Handler) HVals(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if a, err := s.Store().HVals(s.DB(), iconvert(args)...); err != nil {
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
func (h *Handler) HSet(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if x, err := s.Store().HSet(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// HSETNX key field value
func (h *Handler) HSetNX(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if x, err := s.Store().HSetNX(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// HMSET key field value [field value ...]
func (h *Handler) HMSet(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) == 1 || len(args)%2 != 1 {
		return toRespErrorf("len(args) = %d, expect != 1 && mod 2 = 1", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if err := s.Store().HMSet(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

// HMGET key field [field ...]
func (h *Handler) HMGet(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) < 2 {
		return toRespErrorf("len(args) = %d, expect >= 2", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if a, err := s.Store().HMGet(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range a {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}
