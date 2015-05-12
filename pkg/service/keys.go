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
func (h *Handler) Select(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if db, err := store.ParseUint(args[0]); err != nil {
		return toRespError(err)
	} else if db > math.MaxUint32 {
		return toRespErrorf("parse db = %d", db)
	} else {
		s.SetDB(uint32(db))
		return redis.NewString("OK"), nil
	}
}

// DEL key [key ...]
func (h *Handler) Del(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) == 0 {
		return toRespErrorf("len(args) = %d, expect != 1", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if n, err := s.Store().Del(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// DUMP key
func (h *Handler) Dump(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if x, err := s.Store().Dump(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else if dump, err := rdb.EncodeDump(x); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytes(dump), nil
	}
}

// TYPE key
func (h *Handler) Type(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if c, err := s.Store().Type(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString(c.String()), nil
	}
}

// EXISTS key
func (h *Handler) Exists(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if x, err := s.Store().Exists(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// TTL key
func (h *Handler) TTL(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if x, err := s.Store().TTL(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// PTTL key
func (h *Handler) PTTL(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if x, err := s.Store().PTTL(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// PERSIST key
func (h *Handler) Persist(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if x, err := s.Store().Persist(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// EXPIRE key seconds
func (h *Handler) Expire(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if x, err := s.Store().Expire(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// PEXPIRE key milliseconds
func (h *Handler) PExpire(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if x, err := s.Store().PExpire(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// EXPIREAT key timestamp
func (h *Handler) ExpireAt(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if x, err := s.Store().ExpireAt(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// PEXPIREAT key timestamp
func (h *Handler) PExpireAt(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if x, err := s.Store().PExpireAt(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// RESTORE key ttlms value
func (h *Handler) Restore(arg0 interface{}, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	s, err := session(arg0, args)
	if err != nil {
		return toRespError(err)
	}

	if err := s.Store().Restore(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}
