// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"bytes"
	"math"
	"strconv"
	"strings"

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

	negativeInfScore = ScoreInt(math.MinInt64)
	positiveInfScore = ScoreInt(math.MaxInt64)
)

// the binary bigendian for negative score is bigger than positive score
// so we must we a flag before the binary buffer for lexicographical sort
const (
	negativeScoreFlag byte = '<'
	positiveScoreFlag byte = '>'
)

var errTravelBreak = errors.New("break current travel")

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
		} else if len(e.Member) == 0 {
			return 0, errArguments("parse args[%d] failed, empty empty", i*2+2)
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
		return 0, errArguments("len(args) = %d, expect = 3", len(args))
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

// holds a inclusive/exclusive range spec by score comparison
type rangeSpec struct {
	Min ScoreInt
	Max ScoreInt

	// are min or max score exclusive
	MinEx bool
	MaxEx bool
}

func (r *rangeSpec) GteMin(v ScoreInt) bool {
	if r.MinEx {
		return v > r.Min
	} else {
		return v >= r.Min
	}
}

func (r *rangeSpec) LteMax(v ScoreInt) bool {
	if r.MaxEx {
		return v < r.Max
	} else {
		return v <= r.Max
	}
}

func (r *rangeSpec) InRange(v ScoreInt) bool {
	if r.Min > r.Max || (r.Min == r.Max && (r.MinEx || r.MaxEx)) {
		return false
	}

	if !r.GteMin(v) {
		return false
	}

	if !r.LteMax(v) {
		return false
	}
	return true
}

func parseRangeScore(buf []byte) (ScoreInt, bool, error) {
	if len(buf) == 0 {
		return 0, false, errors.Errorf("empty range score argument")
	}

	ex := false
	if buf[0] == '(' {
		buf = buf[1:]
		ex = true
	}

	str := strings.ToLower(string(buf))
	switch str {
	case "-inf":
		return negativeInfScore, ex, nil
	case "+inf":
		return positiveInfScore, ex, nil
	default:
		if score, err := strconv.ParseInt(str, 10, 64); err != nil {
			return 0, ex, errors.Trace(err)
		} else if !isValidScore(ScoreInt(score)) {
			return 0, ex, errors.Errorf("invalid score %v, must in [%d, %d]", score, MinScore, MaxScore)
		} else {
			return ScoreInt(score), ex, nil
		}
	}
}

func parseRange(min []byte, max []byte) (*rangeSpec, error) {
	var r rangeSpec
	var err error

	if r.Min, r.MinEx, err = parseRangeScore(min); err != nil {
		return nil, err
	}

	if r.Max, r.MaxEx, err = parseRangeScore(max); err != nil {
		return nil, err
	}

	return &r, nil
}

// Seek to the last node that is contained in the specified range.
// return true for successful found
func (o *zsetRow) seekFirstInRange(s *Store, it *storeIterator, r *rangeSpec) (bool, error) {
	o.Score = r.Min
	o.Member = []byte{}

	it.SeekTo(o.IndexKey())
	prefixKey := o.IndexKeyPrefix()
	for ; it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, prefixKey) {
			return false, nil
		}

		key = key[len(prefixKey):]

		if err := o.ParseIndexKeySuffix(key); err != nil {
			return false, errors.Trace(err)
		} else if r.GteMin(o.Score) {
			break
		}
	}

	if err := it.Error(); err != nil {
		return false, errors.Trace(err)
	}

	// check score <= max
	return r.LteMax(o.Score), nil
}

// travel zset in range, call f in every iteration.
func (o *zsetRow) travelInRange(s *Store, r *rangeSpec, f func(o *zsetRow) error) error {
	it := s.getIterator()
	defer s.putIterator(it)

	o.Score = r.Min
	o.Member = []byte{}

	it.SeekTo(o.IndexKey())
	prefixKey := o.IndexKeyPrefix()
	for ; it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, prefixKey) {
			return nil
		}

		key = key[len(prefixKey):]

		if err := o.ParseIndexKeySuffix(key); err != nil {
			return errors.Trace(err)
		}

		if r.InRange(o.Score) {
			if err := f(o); err != nil {
				return errors.Trace(err)
			}
		} else if !r.LteMax(o.Score) {
			return nil
		}
	}

	if err := it.Error(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// ZCOUNT key min max
func (s *Store) ZCount(db uint32, args ...interface{}) (int64, error) {
	if len(args) != 3 {
		return 0, errArguments("len(args) = %d, expect = 3", len(args))
	}

	var key []byte
	var min []byte
	var max []byte
	for i, ref := range []interface{}{&key, &min, &max} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	r, err := parseRange(min, max)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil {
		return 0, err
	}

	var count int64 = 0
	f := func(o *zsetRow) error {
		count++
		return nil
	}

	if err = o.travelInRange(s, r, f); err != nil {
		return 0, errors.Trace(err)
	}

	return count, nil
}

type lexRangeSpec struct {
	Min   []byte //use nil for min string
	Max   []byte //use empty slice for max string
	MinEx bool
	MaxEx bool
}

func (r *lexRangeSpec) GteMin(v []byte) bool {
	if r.Min == nil {
		return true
	} else if len(r.Min) == 0 {
		return false
	}

	if r.MinEx {
		return bytes.Compare(v, r.Min) > 0
	} else {
		return bytes.Compare(v, r.Min) >= 0
	}
}

func (r *lexRangeSpec) LteMax(v []byte) bool {
	if r.Max == nil {
		return false
	} else if len(r.Max) == 0 {
		return true
	}

	if r.MaxEx {
		return bytes.Compare(r.Max, v) > 0
	} else {
		return bytes.Compare(r.Max, v) >= 0
	}
}

func (r *lexRangeSpec) InRange(v []byte) bool {
	if bytes.Compare(r.Min, r.Max) == 0 && (r.MinEx || r.MaxEx) {
		return false
	} else if len(r.Min) > 0 && len(r.Max) > 0 && bytes.Compare(r.Min, r.Max) > 0 {
		return false
	}

	if !r.GteMin(v) {
		return false
	}

	if !r.LteMax(v) {
		return false
	}

	return true
}

func parseLexRangeItem(buf []byte) ([]byte, bool, error) {
	if len(buf) == 0 {
		return nil, false, errors.Errorf("empty lex range item")
	}

	ex := false
	var dest []byte

	switch buf[0] {
	case '+':
		if len(buf) > 1 {
			return nil, false, errors.Errorf("invalid lex range item, only +  allowed, but %s", buf)
		}
		dest = []byte{}
	case '-':
		if len(buf) > 1 {
			return nil, false, errors.Errorf("invalid lex range item, only - allowed, but %s", buf)
		}
		dest = nil
	case '(', '[':
		dest = buf[1:]
		if len(dest) == 0 {
			return nil, false, errors.Errorf("invalid empty lex range item %s", buf)
		}
		ex = buf[0] == '('
	default:
		return nil, false, errors.Errorf("invalid lex range item at first byte, %s", buf)
	}

	return dest, ex, nil
}

func parseLexRangeSpec(min []byte, max []byte) (*lexRangeSpec, error) {
	var r lexRangeSpec
	var err error

	r.Min, r.MinEx, err = parseLexRangeItem(min)
	if err != nil {
		return nil, err
	}

	r.Max, r.MaxEx, err = parseLexRangeItem(max)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

// travel zset in lex range, call f in every iteration.
func (o *zsetRow) travelInLexRange(s *Store, r *lexRangeSpec, f func(o *zsetRow) error) error {
	it := s.getIterator()
	defer s.putIterator(it)

	o.Score = MinScore
	o.Member = r.Min

	it.SeekTo(o.IndexKey())
	prefixKey := o.IndexKeyPrefix()
	for ; it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, prefixKey) {
			return nil
		}

		key = key[len(prefixKey):]

		if err := o.ParseIndexKeySuffix(key); err != nil {
			return errors.Trace(err)
		}

		if r.InRange(o.Member) {
			if err := f(o); err == errTravelBreak {
				return nil
			} else if err != nil {
				return errors.Trace(err)
			}
		} else if !r.LteMax(o.Member) {
			return nil
		}
	}

	if err := it.Error(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// ZLEXCOUNT key min max
func (s *Store) ZLexCount(db uint32, args ...interface{}) (int64, error) {
	if len(args) != 3 {
		return 0, errArguments("len(args) = %d, expect = 3", len(args))
	}

	var key []byte
	var min []byte
	var max []byte
	for i, ref := range []interface{}{&key, &min, &max} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	r, err := parseLexRangeSpec(min, max)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil {
		return 0, err
	}

	var count int64 = 0
	firstScore := negativeInfScore
	f := func(o *zsetRow) error {
		count++
		if firstScore == negativeInfScore {
			firstScore = o.Score
		}
		if firstScore != o.Score {
			return errTravelBreak
		}
		return nil
	}

	if err = o.travelInLexRange(s, r, f); err != nil {
		return 0, errors.Trace(err)
	}

	return count, nil
}
