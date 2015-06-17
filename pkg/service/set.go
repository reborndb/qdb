// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import redis "github.com/reborndb/go/redis/resp"

// SADD key member [member ...]
func SAddCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) < 2 {
		return toRespErrorf("len(args) = %d, expect >= 2", len(args))
	}

	if n, err := c.Store().SAdd(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// SCARD key
func SCardCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if n, err := c.Store().SCard(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// SISMEMBER key member
func SIsMemberCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	if x, err := c.Store().SIsMember(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// SMEMBERS key
func SMembersCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if a, err := c.Store().SMembers(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range a {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// SPOP key
func SPopCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if v, err := c.Store().SPop(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytes(v), nil
	}
}

// SRANDMEMBER key [count]
func SRandMemberCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 && len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 1 or 2", len(args))
	}

	if a, err := c.Store().SRandMember(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range a {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// SREM key member [member ...]
func SRemCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) < 2 {
		return toRespErrorf("len(args) = %d, expect >= 2", len(args))
	}

	if n, err := c.Store().SRem(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

func init() {
	Register("sadd", SAddCmd)
	Register("scard", SCardCmd)
	Register("sismember", SIsMemberCmd)
	Register("smembers", SMembersCmd)
	Register("spop", SPopCmd)
	Register("srandmember", SRandMemberCmd)
	Register("srem", SRemCmd)
}
