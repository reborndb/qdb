// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/reborndb/go/redis/rdb"
)

func zdel(t *testing.T, db uint32, key string, expect int64) {
	kdel(t, expect, db, key)
}

func zdump(t *testing.T, db uint32, key string, expect ...interface{}) {
	kexists(t, db, key, 1)
	v, err := testStore.Dump(db, key)
	checkerror(t, err, v != nil)
	x, ok := v.(rdb.ZSet)
	checkerror(t, nil, ok)
	checkerror(t, nil, len(expect)%2 == 0)
	m := make(map[string]int64)
	for i := 0; i < len(expect); i += 2 {
		score, err := ParseInt(expect[i+1])
		checkerror(t, err, true)
		m[fmt.Sprint(expect[i])] = score
	}
	checkerror(t, nil, len(x) == len(m))
	for _, e := range x {
		checkerror(t, nil, m[string(e.Member)] == int64(e.Score))
	}
	zcard(t, db, key, int64(len(m)))
	p, err := testStore.ZGetAll(db, key)
	checkerror(t, err, len(p) == len(m)*2)
	for i := 0; i < len(p); i += 2 {
		s, err := ParseInt(string(p[i+1]))
		checkerror(t, err, m[string(p[i])] == s)
	}
}

func zrestore(t *testing.T, db uint32, key string, ttlms int64, expect ...interface{}) {
	var x rdb.ZSet
	checkerror(t, nil, len(expect)%2 == 0)
	for i := 0; i < len(expect); i += 2 {
		score, err := ParseInt(expect[i+1])
		checkerror(t, err, true)
		x = append(x, &rdb.ZSetElement{Member: []byte(fmt.Sprint(expect[i])), Score: float64(score)})
	}
	dump, err := rdb.EncodeDump(x)
	checkerror(t, err, true)
	err = testStore.Restore(db, key, ttlms, dump)
	checkerror(t, err, true)
	zdump(t, db, key, expect...)
	if ttlms == 0 {
		kpttl(t, db, key, -1)
	} else {
		kpttl(t, db, key, int64(ttlms))
	}
}

func zcard(t *testing.T, db uint32, key string, expect int64) {
	x, err := testStore.ZCard(db, key)
	checkerror(t, err, x == expect)
	if expect == 0 {
		kexists(t, db, key, 0)
	} else {
		kexists(t, db, key, 1)
	}
}

func zrem(t *testing.T, db uint32, key string, expect int64, members ...string) {
	args := []interface{}{key}
	for _, s := range members {
		args = append(args, s)
	}
	x, err := testStore.ZRem(db, args...)
	checkerror(t, err, x == expect)
}

func zadd(t *testing.T, db uint32, key string, expect int64, pairs ...interface{}) {
	args := []interface{}{key}
	for i := 0; i < len(pairs); i += 2 {
		args = append(args, pairs[i+1], pairs[i])
	}
	x, err := testStore.ZAdd(db, args...)
	checkerror(t, err, x == expect)
	for i := 0; i < len(pairs); i += 2 {
		score, err := ParseInt(pairs[i+1])
		checkerror(t, err, true)
		zscore(t, db, key, fmt.Sprint(pairs[i]), score)
	}
}

func zscore(t *testing.T, db uint32, key string, member string, expect int64) {
	x, ok, err := testStore.ZScore(db, key, member)
	checkerror(t, err, ok && x == expect)
}

func zincrby(t *testing.T, db uint32, key string, member string, delta int64, expect int64) {
	x, err := testStore.ZIncrBy(db, key, delta, member)
	checkerror(t, err, x == expect)
}

func TestZAdd(t *testing.T) {
	zadd(t, 0, "zset", 1, "0", 0)
	for i := 0; i < 32; i++ {
		zadd(t, 0, "zset", 1, strconv.Itoa(i), int64(i), strconv.Itoa(i+1), int64(i+1))
	}
	zcard(t, 0, "zset", 33)
	ms := []interface{}{}
	for i := 0; i <= 32; i++ {
		ms = append(ms, strconv.Itoa(i), int64(i))
	}
	zdump(t, 0, "zset", ms...)
	kpexpire(t, 0, "zset", 10, 1)
	sleepms(20)
	zdel(t, 0, "zset", 0)
	checkempty(t)
}

func TestZRem(t *testing.T) {
	for i := 0; i < 32; i++ {
		zadd(t, 0, "zset", 1, strconv.Itoa(i), int64(i))
	}
	m := []string{}
	for i := -32; i < 32; i++ {
		m = append(m, strconv.Itoa(i))
	}
	zrem(t, 0, "zset", 32, append(m, m...)...)
	zcard(t, 0, "zset", 0)
	checkempty(t)
}

func TestZIncrBy(t *testing.T) {
	zincrby(t, 0, "zset", "a", 1, 1)
	zincrby(t, 0, "zset", "a", -1, 0)
	zdump(t, 0, "zset", "a", 0)
	zincrby(t, 0, "zset", "a", 1000, 1000)
	zcard(t, 0, "zset", 1)
	zdel(t, 0, "zset", 1)
	checkempty(t)
}

func TestZRestore(t *testing.T) {
	ms := []interface{}{}
	for i := 0; i < 32; i++ {
		ms = append(ms, strconv.Itoa(i), i*i)
	}
	zrestore(t, 0, "zset", 0, ms...)
	zdump(t, 0, "zset", ms...)
	kpttl(t, 0, "zset", -1)

	for i := 0; i < len(ms); i += 2 {
		ms[i], ms[i+1] = strconv.Itoa(rand.Int()), rand.Int63()%MaxScore
	}
	zrestore(t, 0, "zset", 500, ms...)
	zcard(t, 0, "zset", 32)
	sleepms(1000)
	kpttl(t, 0, "zset", -2)
	zdel(t, 0, "zset", 0)
	checkempty(t)
}
