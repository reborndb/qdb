// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	"strconv"

	. "gopkg.in/check.v1"
)

func (s *testServiceSuite) checkZSet(c *C, k string, expect map[string]float64) {
	ay := s.checkBytesArray(c, "zgetall", k)
	if expect == nil {
		c.Assert(ay, IsNil)
	} else {
		c.Assert(ay, HasLen, len(expect)*2)
		for i := 0; i < len(expect); i++ {
			k := string(ay[i*2])
			v := string(ay[i*2+1])
			f, err := strconv.ParseFloat(v, 64)
			c.Assert(err, IsNil)
			c.Assert((expect[k]-f) < 1e-9, Equals, true)
		}
	}
}

func (s *testServiceSuite) TestZAdd(c *C) {
	k := randomKey(c)
	s.checkInt(c, 1, "zadd", k, 1, "one")
	s.checkInt(c, 2, "zadd", k, 2, "two", 3, "three")
	s.checkInt(c, 1, "zadd", k, 1, "one", 4, "four", 5, "four")
	s.checkZSet(c, k, map[string]float64{"one": 1, "two": 2, "three": 3, "four": 5})
	s.checkInt(c, 0, "zadd", k, 1, "one", 4, "four")
	s.checkZSet(c, k, map[string]float64{"one": 1, "two": 2, "three": 3, "four": 4})
}

func (s *testServiceSuite) TestZCard(c *C) {
	k := randomKey(c)
	s.checkInt(c, 0, "zcard", k)
	s.checkInt(c, 1, "zadd", k, 1, "one")
	s.checkInt(c, 1, "zcard", k)
	s.checkInt(c, 2, "zadd", k, 2, "two", 3, "three")
	s.checkInt(c, 3, "zcard", k)
	s.checkInt(c, 0, "zadd", k, 4, "two")
	s.checkInt(c, 3, "zcard", k)
}

func (s *testServiceSuite) TestZScore(c *C) {
	k := randomKey(c)
	s.checkNil(c, "zscore", k, "one")
	s.checkInt(c, 1, "zadd", k, 1, "one")
	s.checkFloat(c, 1, "zscore", k, "one")
	s.checkNil(c, "zscore", k, "two")
}

func (s *testServiceSuite) TestZRem(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "zadd", k, 1, "key1", 2, "key2", 3, "key3")
	s.checkInt(c, 0, "zrem", k, "key")
	s.checkInt(c, 1, "zrem", k, "key1")
	s.checkZSet(c, k, map[string]float64{"key2": 2, "key3": 3})
	s.checkInt(c, 2, "zrem", k, "key1", "key2", "key3")
	s.checkZSet(c, k, nil)
	s.checkInt(c, -2, "ttl", k)
}

func (s *testServiceSuite) TestZIncrBy(c *C) {
	k := randomKey(c)
	s.checkFloat(c, 1, "zincrby", k, 1, "one")
	s.checkFloat(c, 1, "zincrby", k, 1, "two")
	s.checkFloat(c, 2, "zincrby", k, 1, "two")
	s.checkZSet(c, k, map[string]float64{"one": 1, "two": 2})
}

func (s *testServiceSuite) TestZCount(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "zadd", k, 1, "1", 2, "2", 3, "3")
	s.checkInt(c, 3, "zcount", k, "1", "3")
	s.checkInt(c, 2, "zcount", k, "(1", "3")
	s.checkInt(c, 1, "zcount", k, "(1", "(3")
	s.checkInt(c, 3, "zcount", k, "-inf", "+inf")
}

func (s *testServiceSuite) TestZLexCount(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "zadd", k, 0, "a", 0, "b", 0, "c")
	s.checkInt(c, 1, "zlexcount", k, "[a", "(b")
	s.checkInt(c, 2, "zlexcount", k, "[a", "[b")
	s.checkInt(c, 3, "zlexcount", k, "-", "+")
	s.checkInt(c, 0, "zlexcount", k, "-", "-")
}

func (s *testServiceSuite) checkZRange(c *C, cmd string, expect []string, key string, args ...interface{}) {
	a := append([]interface{}{key}, args...)
	ay := s.checkBytesArray(c, cmd, a...)
	if expect == nil {
		c.Assert(ay, IsNil)
	} else {
		for i := range expect {
			c.Assert(expect[i], Equals, string(ay[i]))
		}
	}
}

func (s *testServiceSuite) TestZRange(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "zadd", k, 0, "a", 1, "b", 2, "c")
	s.checkZRange(c, "zrange", []string{"b", "c"}, k, 1, 2)
	s.checkZRange(c, "zrange", []string{"b", "1", "c", "2"}, k, 1, 2, "WITHSCORES")
	s.checkZRange(c, "zrange", nil, k, 3, 3)
	s.checkZRange(c, "zrange", []string{"b", "c"}, k, -2, -1)
	s.checkZRange(c, "zrevrange", []string{"c", "b", "a"}, k, 0, -1)
	s.checkZRange(c, "zrevrange", []string{"a"}, k, 2, 3)
	s.checkZRange(c, "zrevrange", []string{"b", "a"}, k, -2, -1)
}

func (s *testServiceSuite) TestZRangeByLex(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "zadd", k, 0, "a", 0, "b", 0, "c")
	s.checkZRange(c, "zrangebylex", []string{"a", "b", "c"}, k, "[a", "[c")
	s.checkZRange(c, "zrangebylex", nil, k, "-", "-")
	s.checkZRange(c, "zrangebylex", []string{"a"}, k, "[a", "[c", "LIMIT", 0, 1)
	s.checkZRange(c, "zrevrangebylex", []string{"c"}, k, "[c", "[b", "LIMIT", 0, 1)
	s.checkZRange(c, "zrevrangebylex", []string{"b", "a"}, k, "(c", "[a", "LIMIT", 0, -1)
	s.checkZRange(c, "zrevrangebylex", []string{"c"}, k, "+", "-", "LIMIT", 0, 1)
}

func (s *testServiceSuite) TestZRangeByScore(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "zadd", k, 1, "a", 2, "b", 3, "c")
	s.checkZRange(c, "zrangebyscore", []string{"a", "b", "c"}, k, "1", "3")
	s.checkZRange(c, "zrangebyscore", nil, k, "-inf", "-1")
	s.checkZRange(c, "zrangebyscore", []string{"a"}, k, "1", "3", "LIMIT", 0, 1)
	s.checkZRange(c, "zrangebyscore", []string{"b", "2"}, k, "1", "3", "LIMIT", 1, 1, "WITHSCORES")
	s.checkZRange(c, "zrevrangebyscore", []string{"c"}, k, "3", "1", "LIMIT", 0, 1)
	s.checkZRange(c, "zrevrangebyscore", []string{"b", "2"}, k, "3", "1", "LIMIT", 1, 1, "WITHSCORES")
}

func (s *testServiceSuite) TestZRank(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "zadd", k, 1, "a", 2, "b", 3, "c")

	s.checkInt(c, 1, "zrank", k, "b")
	s.checkInt(c, 2, "zrank", k, "c")
	s.checkBytes(c, nil, "zrank", k, "d")
	s.checkBytes(c, nil, "zrevrank", k, "d")
	s.checkInt(c, 0, "zrevrank", k, "c")
	s.checkInt(c, 2, "zrevrank", k, "a")
}

func (s *testServiceSuite) TestZRemRangeByLex(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "zadd", k, 0, "a", 0, "b", 0, "c")

	s.checkInt(c, 1, "zremrangebylex", k, "[a", "(b")
	s.checkInt(c, 0, "zremrangebylex", k, "[a", "(b")
	s.checkInt(c, 2, "zremrangebylex", k, "-", "+")
	s.checkInt(c, 0, "zremrangebylex", k, "-", "+")
}

func (s *testServiceSuite) TestZRemRangeByRank(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "zadd", k, 1, "a", 2, "b", 3, "c")

	s.checkInt(c, 1, "zremrangebyrank", k, "0", "0")
	s.checkInt(c, 2, "zremrangebyrank", k, "0", "2")
	s.checkInt(c, 0, "zremrangebyrank", k, "3", "2")
}

func (s *testServiceSuite) TestZRemRangeByScore(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "zadd", k, 1, "a", 2, "b", 3, "c")

	s.checkInt(c, 1, "zremrangebyscore", k, "1", "(2")
	s.checkInt(c, 2, "zremrangebyscore", k, "2", "+inf")
	s.checkInt(c, 0, "zremrangebyscore", k, "-inf", "+inf")
}
