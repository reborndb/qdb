// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	redis "github.com/reborndb/go/redis/resp"
	"github.com/reborndb/qdb/pkg/store"
)

// ZGETALL key
func (h *Handler) ZGetAll(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if a, err := s.Store().ZGetAll(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range a {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// ZCARD key
func (h *Handler) ZCard(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if n, err := s.Store().ZCard(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// ZADD key score member [score member ...]
func (h *Handler) ZAdd(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) == 1 || len(args)%2 != 1 {
		return toRespErrorf("len(args) = %d, expect != 1 && mod 2 = 1", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if n, err := s.Store().ZAdd(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// ZREM key member [member ...]
func (h *Handler) ZRem(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) < 2 {
		return toRespErrorf("len(args) = %d, expect >= 2", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if n, err := s.Store().ZRem(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// ZSCORE key member
func (h *Handler) ZScore(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if v, ok, err := s.Store().ZScore(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else if !ok {
		return redis.NewBulkBytes(nil), nil
	} else {
		return redis.NewBulkBytes(store.FormatInt(v)), nil
	}
}

// ZINCRBY key delta member
func (h *Handler) ZIncrBy(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if v, err := s.Store().ZIncrBy(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytes(store.FormatInt(v)), nil
	}
}

// ZCOUNT key min max
func (h *Handler) ZCount(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if v, err := s.Store().ZCount(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// ZLEXCOUNT key min max
func (h *Handler) ZLexCount(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if v, err := s.Store().ZLexCount(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// ZRANGE key start stop [WITHSCORES]
func (h *Handler) ZRange(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 && len(args) != 4 {
		return toRespErrorf("len(args) = %d, expect = 3 or 4", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if ay, err := s.Store().ZRange(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range ay {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// ZRANGEBYLEX key start stop [LIMIT offset count]
func (h *Handler) ZRangeByLex(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 && len(args) != 6 {
		return toRespErrorf("len(args) = %d, expect = 3 or 6", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if ay, err := s.Store().ZRangeByLex(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range ay {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}
