// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	redis "github.com/reborndb/go/redis/resp"
	"github.com/reborndb/qdb/pkg/store"
)

// GET key
func GetCmd(s Session, args [][]byte) (redis.Resp, error) {
	if b, err := s.Store().Get(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytes(b), nil
	}
}

// APPEND key value
func AppendCmd(s Session, args [][]byte) (redis.Resp, error) {
	if n, err := s.Store().Append(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// SET key value [EX seconds] [PX milliseconds] [NX|XX]
func SetCmd(s Session, args [][]byte) (redis.Resp, error) {
	if err := s.Store().Set(s.DB(), args); err != nil && err != store.ErrSetAborted {
		return toRespError(err)
	} else if err == store.ErrSetAborted {
		return redis.NewBulkBytes(nil), nil
	} else {
		return redis.NewString("OK"), nil
	}
}

// PSETEX key milliseconds value
func PSetEXCmd(s Session, args [][]byte) (redis.Resp, error) {
	if err := s.Store().PSetEX(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

// SETEX key seconds value
func SetEXCmd(s Session, args [][]byte) (redis.Resp, error) {
	if err := s.Store().SetEX(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

// SETNX key value
func SetNXCmd(s Session, args [][]byte) (redis.Resp, error) {
	if n, err := s.Store().SetNX(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// GETSET key value
func GetSetCmd(s Session, args [][]byte) (redis.Resp, error) {
	if b, err := s.Store().GetSet(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytes(b), nil
	}
}

// INCR key
func IncrCmd(s Session, args [][]byte) (redis.Resp, error) {
	if v, err := s.Store().Incr(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// INCRBY key delta
func IncrByCmd(s Session, args [][]byte) (redis.Resp, error) {
	if v, err := s.Store().IncrBy(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// DECR key
func DecrCmd(s Session, args [][]byte) (redis.Resp, error) {
	if v, err := s.Store().Decr(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// DECRBY key delta
func DecrByCmd(s Session, args [][]byte) (redis.Resp, error) {
	if v, err := s.Store().DecrBy(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// INCRBYFLOAT key delta
func IncrByFloatCmd(s Session, args [][]byte) (redis.Resp, error) {
	if v, err := s.Store().IncrByFloat(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytesWithString(store.FormatFloatString(v)), nil
	}
}

// SETBIT key offset value
func SetBitCmd(s Session, args [][]byte) (redis.Resp, error) {
	if x, err := s.Store().SetBit(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// SETRANGE key offset value
func SetRangeCmd(s Session, args [][]byte) (redis.Resp, error) {
	if x, err := s.Store().SetRange(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// MSET key value [key value ...]
func MSetCmd(s Session, args [][]byte) (redis.Resp, error) {
	if err := s.Store().MSet(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

// MSETNX key value [key value ...]
func MSetNXCmd(s Session, args [][]byte) (redis.Resp, error) {
	if n, err := s.Store().MSetNX(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// MGET key [key ...]
func MGetCmd(s Session, args [][]byte) (redis.Resp, error) {
	if a, err := s.Store().MGet(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range a {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// STRLEN key
func StrlenCmd(s Session, args [][]byte) (redis.Resp, error) {
	if n, err := s.Store().Strlen(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

func init() {
	Register("append", AppendCmd, CmdWrite)
	Register("decr", DecrCmd, CmdWrite)
	Register("decrby", DecrByCmd, CmdWrite)
	Register("get", GetCmd, CmdReadonly)
	Register("getset", GetSetCmd, CmdWrite)
	Register("incr", IncrCmd, CmdWrite)
	Register("incrby", IncrByCmd, CmdWrite)
	Register("incrbyfloat", IncrByFloatCmd, CmdWrite)
	Register("mget", MGetCmd, CmdReadonly)
	Register("mset", MSetCmd, CmdWrite)
	Register("msetnx", MSetNXCmd, CmdWrite)
	Register("psetex", PSetEXCmd, CmdWrite)
	Register("set", SetCmd, CmdWrite)
	Register("setbit", SetBitCmd, CmdWrite)
	Register("setex", SetEXCmd, CmdWrite)
	Register("setnx", SetNXCmd, CmdWrite)
	Register("setrange", SetRangeCmd, CmdWrite)
	Register("strlen", StrlenCmd, CmdReadonly)
}
