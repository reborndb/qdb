// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	redis "github.com/reborndb/go/redis/resp"
	"github.com/reborndb/qdb/pkg/store"
)

// ZGETALL key
func ZGetAllCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if a, err := c.Store().ZGetAll(c.DB(), iconvert(args)...); err != nil {
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
func ZCardCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if n, err := c.Store().ZCard(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// ZADD key score member [score member ...]
func ZAddCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) == 1 || len(args)%2 != 1 {
		return toRespErrorf("len(args) = %d, expect != 1 && mod 2 = 1", len(args))
	}

	if n, err := c.Store().ZAdd(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// ZREM key member [member ...]
func ZRemCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) < 2 {
		return toRespErrorf("len(args) = %d, expect >= 2", len(args))
	}

	if n, err := c.Store().ZRem(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// ZSCORE key member
func ZScoreCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	if v, ok, err := c.Store().ZScore(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else if !ok {
		return redis.NewBulkBytes(nil), nil
	} else {
		return redis.NewBulkBytes(store.FormatFloat(v)), nil
	}
}

// ZINCRBY key delta member
func ZIncrByCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	if v, err := c.Store().ZIncrBy(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytes(store.FormatFloat(v)), nil
	}
}

// ZCOUNT key min max
func ZCountCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	if v, err := c.Store().ZCount(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// ZLEXCOUNT key min max
func ZLexCountCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	if v, err := c.Store().ZLexCount(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// ZRANGE key start stop [WITHSCORES]
func ZRangeCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 && len(args) != 4 {
		return toRespErrorf("len(args) = %d, expect = 3 or 4", len(args))
	}

	if ay, err := c.Store().ZRange(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range ay {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// ZREVRANGE key start stop [WITHSCORES]
func ZRevRangeCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 && len(args) != 4 {
		return toRespErrorf("len(args) = %d, expect = 3 or 4", len(args))
	}

	if ay, err := c.Store().ZRevRange(c.DB(), iconvert(args)...); err != nil {
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
func ZRangeByLexCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 && len(args) != 6 {
		return toRespErrorf("len(args) = %d, expect = 3 or 6", len(args))
	}

	if ay, err := c.Store().ZRangeByLex(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range ay {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// ZREVRANGEBYLEX key start stop [LIMIT offset count]
func ZRevRangeByLexCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 && len(args) != 6 {
		return toRespErrorf("len(args) = %d, expect = 3 or 6", len(args))
	}

	if ay, err := c.Store().ZRevRangeByLex(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range ay {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
func ZRangeByScoreCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) < 3 {
		return toRespErrorf("len(args) = %d, expect >= 3", len(args))
	}

	if ay, err := c.Store().ZRangeByScore(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range ay {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// ZREVRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
func ZRevRangeByScoreCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) < 3 {
		return toRespErrorf("len(args) = %d, expect >= 3", len(args))
	}

	if ay, err := c.Store().ZRevRangeByScore(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range ay {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// ZRANK key member
func ZRankCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect 2", len(args))
	}

	if v, err := c.Store().ZRank(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else if v >= 0 {
		return redis.NewInt(v), nil
	} else {
		return redis.NewBulkBytes(nil), nil
	}
}

// ZREVRANK key member
func ZRevRankCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect 2", len(args))
	}

	if v, err := c.Store().ZRevRank(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else if v >= 0 {
		return redis.NewInt(v), nil
	} else {
		return redis.NewBulkBytes(nil), nil
	}
}

// ZREMRANGEBYLEX key min max
func ZRemRangeByLexCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect 3", len(args))
	}

	if v, err := c.Store().ZRemRangeByLex(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// ZREMRANGEBYRANK key start stop
func ZRemRangeByRankCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect 3", len(args))
	}

	if v, err := c.Store().ZRemRangeByRank(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// ZREMRANGEBYSCORE key min max
func ZRemRangeByScoreCmd(c *conn, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect 3", len(args))
	}

	if v, err := c.Store().ZRemRangeByScore(c.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

func init() {
	Register("zgetall", ZGetAllCmd)
	Register("zcard", ZCardCmd)
	Register("zadd", ZAddCmd)
	Register("zrem", ZRemCmd)
	Register("zscore", ZScoreCmd)
	Register("zincrby", ZIncrByCmd)
	Register("zcount", ZCountCmd)
	Register("zlexcount", ZLexCountCmd)
	Register("zrange", ZRangeCmd)
	Register("zrevrange", ZRevRangeCmd)
	Register("zrangebylex", ZRangeByLexCmd)
	Register("zrevrangebylex", ZRevRangeByLexCmd)
	Register("zrangebyscore", ZRangeByScoreCmd)
	Register("zrevrangebyscore", ZRevRangeByScoreCmd)
	Register("zrank", ZRankCmd)
	Register("zrevrank", ZRevRankCmd)
	Register("zremrangebylex", ZRemRangeByLexCmd)
	Register("zremrangebyrank", ZRemRangeByRankCmd)
	Register("zremrangebyscore", ZRemRangeByScoreCmd)
}
