// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"bytes"

	"github.com/reborndb/go/errors"
	"github.com/reborndb/go/redis/rdb"
	"github.com/reborndb/qdb/pkg/engine"
)

type ScoreInt int64

const (
	// we will use int64 as the score for backend binary sort
	// but redis rdb will use float64
	// to avoid data precison lost, the int64 range must be in [- 2**53, 2**53] like javascript
	MaxScore = ScoreInt(1 << 53)
	MinScore = -(ScoreInt(1 << 53))
)

// the binary bigendian for negative score is bigger than positive score
// so we must we a flag before the binary buffer for lexicographical sort
const (
	negativeScoreFlag byte = '<'
	positiveScoreFlag byte = '>'
)

type zsetRow struct {
	*storeRowHelper

	Size   int64
	Member []byte
	Score  ScoreInt

	indexKeyPrefix []byte
	indexKeyRefs   []interface{}
	indexValueRefs []interface{}
}

func encodeIndexKeyPrefix(db uint32, key []byte) []byte {
	w := NewBufWriter(nil)
	encodeRawBytes(w, indexCode, &db, &key)
	return w.Bytes()
}

func newZSetRow(db uint32, key []byte) *zsetRow {
	o := &zsetRow{}
	o.lazyInit(db, key, newStoreRowHelper(db, key, ZSetCode))
	return o
}

func isValidScore(score ScoreInt) bool {
	return score >= MinScore && score <= MaxScore
}

func (o *zsetRow) lazyInit(db uint32, key []byte, h *storeRowHelper) {
	o.storeRowHelper = h
	o.metaValueRefs = []interface{}{&o.Size}

	o.dataKeyRefs = []interface{}{&o.Member}
	o.dataValueRefs = []interface{}{&o.Score}

	o.indexKeyPrefix = encodeIndexKeyPrefix(db, key)
	o.indexKeyRefs = []interface{}{&o.Score, &o.Member}
	o.indexValueRefs = nil
}

func (o *zsetRow) IndexKeyPrefix() []byte {
	return o.indexKeyPrefix
}

func (o *zsetRow) IndexKey() []byte {
	w := NewBufWriter(o.IndexKeyPrefix())

	if o.Score >= 0 {
		encodeRawBytes(w, positiveScoreFlag)
	} else {
		encodeRawBytes(w, negativeScoreFlag)
	}

	encodeRawBytes(w, o.indexKeyRefs...)
	return w.Bytes()
}

func (o *zsetRow) IndexValue() []byte {
	w := NewBufWriter(nil)
	encodeRawBytes(w, o.code)
	encodeRawBytes(w, o.indexValueRefs...)
	return w.Bytes()
}

func (o *zsetRow) ParseIndexKeySuffix(p []byte) (err error) {
	r := NewBufReader(p)
	var scoreFlag byte
	err = decodeRawBytes(r, err, &scoreFlag)
	err = decodeRawBytes(r, err, o.indexKeyRefs...)
	err = decodeRawBytes(r, err)
	return
}

func (o *zsetRow) ParseIndexValue(p []byte) (err error) {
	r := NewBufReader(p)
	err = decodeRawBytes(r, err, o.code)
	err = decodeRawBytes(r, err, o.indexValueRefs...)
	err = decodeRawBytes(r, err)
	return
}

func (o *zsetRow) LoadIndexValue(r storeReader) (bool, error) {
	p, err := r.getRowValue(o.IndexKey())
	if err != nil || p == nil {
		return false, err
	}
	return true, o.ParseIndexValue(p)
}

func (o *zsetRow) TestIndexValue(r storeReader) (bool, error) {
	p, err := r.getRowValue(o.IndexKey())
	if err != nil || p == nil {
		return false, err
	}
	return true, nil
}

func (o *zsetRow) deleteObject(s *Store, bt *engine.Batch) error {
	it := s.getIterator()
	defer s.putIterator(it)
	for pfx := it.SeekTo(o.DataKeyPrefix()); it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, pfx) {
			break
		}
		bt.Del(key)
	}

	for pfx := it.SeekTo(o.IndexKeyPrefix()); it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, pfx) {
			break
		}
		bt.Del(key)
	}

	bt.Del(o.MetaKey())
	return it.Error()
}

func (o *zsetRow) storeObject(s *Store, bt *engine.Batch, expireat uint64, obj interface{}) error {
	zset, ok := obj.(rdb.ZSet)
	if !ok || len(zset) == 0 {
		return errors.Trace(ErrObjectValue)
	}
	for i, e := range zset {
		if e == nil {
			return errArguments("zset[%d] is nil", i)
		}
		if len(e.Member) == 0 {
			return errArguments("zset[%d], len(member) = %d", i, len(e.Member))
		}
	}

	ms := &markSet{}
	for _, e := range zset {
		o.Member, o.Score = e.Member, ScoreInt(e.Score)
		if !isValidScore(o.Score) {
			return errors.Errorf("invalid score %v, must in [%d, %d]", e.Score, MinScore, MaxScore)
		}

		ms.Set(o.Member)
		bt.Set(o.DataKey(), o.DataValue())
		bt.Set(o.IndexKey(), o.IndexValue())
	}
	o.Size, o.ExpireAt = ms.Len(), expireat
	bt.Set(o.MetaKey(), o.MetaValue())
	return nil
}

func (o *zsetRow) loadObjectValue(r storeReader) (interface{}, error) {
	zset := make([]*rdb.ZSetElement, 0, o.Size)
	it := r.getIterator()
	defer r.putIterator(it)
	for pfx := it.SeekTo(o.DataKeyPrefix()); it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, pfx) {
			break
		}
		sfx := key[len(pfx):]
		if err := o.ParseDataKeySuffix(sfx); err != nil {
			return nil, err
		}
		if err := o.ParseDataValue(it.Value()); err != nil {
			return nil, err
		}
		zset = append(zset, &rdb.ZSetElement{Member: o.Member, Score: float64(o.Score)})
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if o.Size == 0 || int64(len(zset)) != o.Size {
		return nil, errors.Errorf("len(zset) = %d, zset.size = %d", len(zset), o.Size)
	}
	return rdb.ZSet(zset), nil
}

func (s *Store) loadZSetRow(db uint32, key []byte, deleteIfExpired bool) (*zsetRow, error) {
	o, err := s.loadStoreRow(db, key, deleteIfExpired)
	if err != nil {
		return nil, err
	} else if o != nil {
		x, ok := o.(*zsetRow)
		if ok {
			return x, nil
		}
		return nil, errors.Trace(ErrNotZSet)
	}
	return nil, nil
}

// ZGETALL key
func (s *Store) ZGetAll(db uint32, args ...interface{}) ([][]byte, error) {
	if len(args) != 1 {
		return nil, errArguments("len(args) = %d, expect = 1", len(args))
	}

	var key []byte
	for i, ref := range []interface{}{&key} {
		if err := parseArgument(args[i], ref); err != nil {
			return nil, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil || o == nil {
		return nil, err
	}

	x, err := o.loadObjectValue(s)
	if err != nil || x == nil {
		return nil, err
	}

	eles := x.(rdb.ZSet)
	rets := make([][]byte, len(eles)*2)
	for i, e := range eles {
		rets[i*2], rets[i*2+1] = e.Member, FormatInt(int64(e.Score))
	}
	return rets, nil
}

// ZCARD key
func (s *Store) ZCard(db uint32, args ...interface{}) (int64, error) {
	if len(args) != 1 {
		return 0, errArguments("len(args) = %d, expect = 1", len(args))
	}

	var key []byte
	for i, ref := range []interface{}{&key} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil || o == nil {
		return 0, err
	}
	return o.Size, nil
}

// ZADD key score member [score member ...]
func (s *Store) ZAdd(db uint32, args ...interface{}) (int64, error) {
	if len(args) == 1 || len(args)%2 != 1 {
		return 0, errArguments("len(args) = %d, expect != 1 && mod 2 = 1", len(args))
	}

	var key []byte
	var eles = make([]struct {
		Member []byte
		Score  int64
	}, len(args)/2)
	if err := parseArgument(args[0], &key); err != nil {
		return 0, errArguments("parse args[%d] failed, %s", 0, err)
	}
	for i := 0; i < len(eles); i++ {
		e := &eles[i]
		if err := parseArgument(args[i*2+1], &e.Score); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i*2+1, err)
		} else if !isValidScore(ScoreInt(e.Score)) {
			return 0, errArguments("parse args[%d] failed, invalid score %d", i*2+1, e.Score)
		}
		if err := parseArgument(args[i*2+2], &e.Member); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i*2+2, err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil {
		return 0, err
	}

	if o == nil {
		o = newZSetRow(db, key)
	}

	ms := &markSet{}
	bt := engine.NewBatch()
	for _, e := range eles {
		o.Member = e.Member
		exists, err := o.LoadDataValue(s)
		if err != nil {
			return 0, err
		}
		if !exists {
			ms.Set(o.Member)
		} else {
			// if old exists, remove index key first
			bt.Del(o.IndexKey())
		}

		o.Score = ScoreInt(e.Score)

		bt.Set(o.DataKey(), o.DataValue())
		bt.Set(o.IndexKey(), o.IndexValue())
	}

	n := ms.Len()
	if n != 0 {
		o.Size += n
		bt.Set(o.MetaKey(), o.MetaValue())
	}
	fw := &Forward{DB: db, Op: "ZAdd", Args: args}
	return n, s.commit(bt, fw)
}

// ZREM key member [member ...]
func (s *Store) ZRem(db uint32, args ...interface{}) (int64, error) {
	if len(args) < 2 {
		return 0, errArguments("len(args) = %d, expect >= 2", len(args))
	}

	var key []byte
	var members = make([][]byte, len(args)-1)
	if err := parseArgument(args[0], &key); err != nil {
		return 0, errArguments("parse args[%d] failed, %s", 0, err)
	}
	for i := 0; i < len(members); i++ {
		if err := parseArgument(args[i+1], &members[i]); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i+1, err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil || o == nil {
		return 0, err
	}

	ms := &markSet{}
	bt := engine.NewBatch()
	for _, o.Member = range members {
		if !ms.Has(o.Member) {
			exists, err := o.LoadDataValue(s)
			if err != nil {
				return 0, err
			}
			if exists {
				bt.Del(o.DataKey())
				bt.Del(o.IndexKey())
				ms.Set(o.Member)
			}
		}
	}

	n := ms.Len()
	if n != 0 {
		if o.Size -= n; o.Size > 0 {
			bt.Set(o.MetaKey(), o.MetaValue())
		} else {
			bt.Del(o.MetaKey())
		}
	}
	fw := &Forward{DB: db, Op: "ZRem", Args: args}
	return n, s.commit(bt, fw)
}

// ZSCORE key member
func (s *Store) ZScore(db uint32, args ...interface{}) (int64, bool, error) {
	if len(args) != 2 {
		return 0, false, errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key, member []byte
	for i, ref := range []interface{}{&key, &member} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, false, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, false, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil || o == nil {
		return 0, false, err
	}

	o.Member = member
	exists, err := o.LoadDataValue(s)
	if err != nil || !exists {
		return 0, false, err
	} else {
		return int64(o.Score), true, nil
	}
}

// ZINCRBY key delta member
func (s *Store) ZIncrBy(db uint32, args ...interface{}) (int64, error) {
	if len(args) != 3 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key, member []byte
	var delta int64
	for i, ref := range []interface{}{&key, &delta, &member} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil {
		return 0, err
	}

	bt := engine.NewBatch()

	var exists bool = false
	if o != nil {
		o.Member = member
		exists, err = o.LoadDataValue(s)
		if err != nil {
			return 0, err
		} else if exists {
			bt.Del(o.IndexKey())
		}
	} else {
		o = newZSetRow(db, key)
		o.Member = member
	}

	if exists {
		delta += int64(o.Score)
	} else {
		o.Size++
		bt.Set(o.MetaKey(), o.MetaValue())
	}
	o.Score = ScoreInt(delta)
	if !isValidScore(o.Score) {
		return 0, errors.Errorf("invalid score %d, must in [%d, %d]", o.Score, MinScore, MaxScore)
	}

	bt.Set(o.DataKey(), o.DataValue())
	bt.Set(o.IndexKey(), o.IndexValue())

	fw := &Forward{DB: db, Op: "ZIncrBy", Args: args}
	return delta, s.commit(bt, fw)
}
