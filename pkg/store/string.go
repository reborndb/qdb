// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/juju/errors"
	"github.com/reborndb/go/errors2"
	"github.com/reborndb/go/redis/rdb"
	"github.com/reborndb/qdb/pkg/engine"
)

const (
	BitAND = "and"
	BitOR  = "or"
	BitXOR = "xor"
	BitNot = "not"
)

var ErrSetAborted = errors.New("SET flow is aborted because of NX|XX condition met")

func adjustIndex(index int64, min, max int64) int64 {
	if index >= 0 {
		return index + min
	} else {
		return index + max
	}
}

func minIntValue(v1, v2 int64) int64 {
	if v1 < v2 {
		return v1
	} else {
		return v2
	}
}

func maxIntValue(v1, v2 int64) int64 {
	if v1 < v2 {
		return v2
	} else {
		return v1
	}
}

var bitsInByte = [256]int32{0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3,
	4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3,
	3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4,
	5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4,
	3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4,
	5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2,
	2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3,
	4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4,
	5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6,
	6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5,
	6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8}

func numberBitCount(i uint32) uint32 {
	i = i - ((i >> 1) & 0x55555555)
	i = (i & 0x33333333) + ((i >> 2) & 0x33333333)
	return (((i + (i >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24
}

type stringRow struct {
	*storeRowHelper

	Value []byte
}

func newStringRow(db uint32, key []byte) *stringRow {
	o := &stringRow{}
	o.lazyInit(db, key, newStoreRowHelper(db, key, StringCode))
	return o
}

func (o *stringRow) lazyInit(db uint32, key []byte, h *storeRowHelper) {
	o.storeRowHelper = h
	o.dataKeyRefs = nil
	o.metaValueRefs = nil
	o.dataValueRefs = []interface{}{&o.Value}
}

func (o *stringRow) deleteObject(s *Store, bt *engine.Batch) error {
	bt.Del(o.DataKey())
	bt.Del(o.MetaKey())
	return nil
}

func (o *stringRow) storeObject(s *Store, bt *engine.Batch, expireat int64, obj interface{}) error {
	value, ok := obj.(rdb.String)
	if !ok || len(value) == 0 {
		return errors.Trace(ErrObjectValue)
	}

	o.ExpireAt, o.Value = expireat, value
	bt.Set(o.DataKey(), o.DataValue())
	bt.Set(o.MetaKey(), o.MetaValue())
	return nil
}

func (o *stringRow) loadObjectValue(r storeReader) (interface{}, error) {
	_, err := o.LoadDataValue(r)
	if err != nil {
		return nil, err
	}

	return rdb.String(o.Value), nil
}

func (s *Store) loadStringRow(db uint32, key []byte, delExp bool) (*stringRow, error) {
	o, err := s.loadStoreRow(db, key, delExp)
	if err != nil {
		return nil, err
	} else if o != nil {
		x, ok := o.(*stringRow)
		if ok {
			return x, nil
		}
		return nil, errors.Trace(ErrNotString)
	}
	return nil, nil
}

// GET key
func (s *Store) Get(db uint32, args [][]byte) ([]byte, error) {
	if len(args) != 1 {
		return nil, errArguments("len(args) = %d, expect = 1", len(args))
	}

	key := args[0]

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	o, err := s.loadStringRow(db, key, false)
	if err != nil || o == nil {
		return nil, err
	} else {
		_, err := o.LoadDataValue(s)
		if err != nil {
			return nil, err
		}

		return o.Value, nil
	}
}

// APPEND key value
func (s *Store) Append(db uint32, args [][]byte) (int64, error) {
	if len(args) != 2 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	key := args[0]
	value := args[1]

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadStringRow(db, key, true)
	if err != nil {
		return 0, err
	}

	bt := engine.NewBatch()
	if o != nil {
		_, err := o.LoadDataValue(s)
		if err != nil {
			return 0, err
		}
		o.Value = append(o.Value, value...)
	} else {
		o = newStringRow(db, key)
		o.Value = value
		bt.Set(o.MetaKey(), o.MetaValue())
	}

	bt.Set(o.DataKey(), o.DataValue())
	fw := &Forward{DB: db, Op: "Append", Args: args}

	return int64(len(o.Value)), s.commit(bt, fw)
}

const (
	setNXFlag uint8 = 1 << 0
	setXXFlag uint8 = 1 << 1
)

// SET key value [EX seconds] [PX milliseconds] [NX|XX]
func (s *Store) Set(db uint32, args [][]byte) error {
	if len(args) < 2 {
		return errArguments("len(args) = %d, expect >= 2", len(args))
	}

	key := args[0]
	value := args[1]

	expireat := int64(0)
	flag := uint8(0)

	for i := 2; i < len(args); {
		switch strings.ToUpper(string(args[i])) {
		case "EX":
			if i+1 >= len(args) {
				return errArguments("invalid set argument for EX")
			}
			ttls, err := ParseInt(args[i+1])
			if err != nil {
				return errArguments("parse EX arg failed %v", err)
			}

			if v, ok := TTLsToExpireAt(ttls); ok && v > 0 {
				expireat = v
			} else {
				return errArguments("invalid EX seconds = %d", ttls)
			}
			i += 2
		case "PX":
			if i+1 >= len(args) {
				return errArguments("invalid set argument for PX")
			}
			ttlms, err := ParseInt(args[i+1])
			if err != nil {
				return errArguments("parse PX arg failed %v", err)
			}
			if v, ok := TTLmsToExpireAt(ttlms); ok && v > 0 {
				expireat = v
			} else {
				return errArguments("invalid PX milliseconds = %d", ttlms)
			}
			i += 2
		case "NX":
			flag |= setNXFlag
			i++
		case "XX":
			flag |= setXXFlag
			i++
		default:
			return errArguments("invalid set argument at %d", i)
		}
	}

	if err := s.acquire(); err != nil {
		return err
	}
	defer s.release()

	bt := engine.NewBatch()

	if o, err := loadStoreRow(s, db, key); err != nil {
		return err
	} else {
		// handle NX and XX flag
		// NX: key is nil or expired
		// XX: key is not nil and not expired
		// otherwise, abort
		if (flag&setNXFlag > 0) && (o != nil && !o.IsExpired()) {
			return ErrSetAborted
		} else if (flag&setXXFlag > 0) && (o == nil || o.IsExpired()) {
			return ErrSetAborted
		}

		// if we are string type, we will overwrite it directly
		// if not, we may delete it first
		if o != nil && o.Code() != StringCode {
			if err := o.deleteObject(s, bt); err != nil {
				return err
			}
		}
	}

	no := newStringRow(db, key)
	no.Value = value
	no.ExpireAt = expireat
	bt.Set(no.DataKey(), no.DataValue())
	bt.Set(no.MetaKey(), no.MetaValue())

	fw := &Forward{DB: db, Op: "Set", Args: args}
	return s.commit(bt, fw)
}

// PSETEX key milliseconds value
func (s *Store) PSetEX(db uint32, args [][]byte) error {
	if len(args) != 3 {
		return errArguments("len(args) = %d, expect = 3", len(args))
	}

	key := args[0]
	milliseconds := args[1]
	value := args[2]

	return s.Set(db, [][]byte{key, value, []byte("PX"), milliseconds})
}

// SETEX key seconds value
func (s *Store) SetEX(db uint32, args [][]byte) error {
	if len(args) != 3 {
		return errArguments("len(args) = %d, expect = 3", len(args))
	}

	key := args[0]
	seconds := args[1]
	value := args[2]

	return s.Set(db, [][]byte{key, value, []byte("EX"), seconds})
}

// SETNX key value
func (s *Store) SetNX(db uint32, args [][]byte) (int64, error) {
	if len(args) != 2 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	key := args[0]
	value := args[1]

	err := s.Set(db, [][]byte{key, value, []byte("NX")})

	if err != nil {
		// key exists
		if errors2.ErrorEqual(err, ErrSetAborted) {
			return 0, nil
		} else {
			return 0, err
		}
	}

	return 1, nil
}

// GETSET key value
func (s *Store) GetSet(db uint32, args [][]byte) ([]byte, error) {
	if len(args) != 2 {
		return nil, errArguments("len(args) = %d, expect = 2", len(args))
	}

	key := args[0]
	value := args[1]

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	o, err := s.loadStringRow(db, key, true)
	if err != nil {
		return nil, err
	}

	bt := engine.NewBatch()
	if o != nil {
		_, err := o.LoadDataValue(s)
		if err != nil {
			return nil, err
		}

		if o.ExpireAt != 0 {
			o.ExpireAt = 0
			bt.Set(o.MetaKey(), o.MetaValue())
		}
	} else {
		o = newStringRow(db, key)
		bt.Set(o.MetaKey(), o.MetaValue())
	}

	o.Value, value = value, o.Value
	bt.Set(o.DataKey(), o.DataValue())

	fw := &Forward{DB: db, Op: "Set", Args: args}
	return value, s.commit(bt, fw)
}

func (s *Store) incrInt(db uint32, key []byte, delta int64) (int64, error) {
	o, err := s.loadStringRow(db, key, true)
	if err != nil {
		return 0, err
	}

	bt := engine.NewBatch()
	if o != nil {
		_, err := o.LoadDataValue(s)
		if err != nil {
			return 0, err
		}
		v, err := ParseInt(o.Value)
		if err != nil {
			return 0, err
		}
		delta += v
	} else {
		o = newStringRow(db, key)
		bt.Set(o.MetaKey(), o.MetaValue())
	}

	o.Value = FormatInt(delta)
	bt.Set(o.DataKey(), o.DataValue())

	fw := &Forward{DB: db, Op: "IncrBy", Args: [][]byte{key, FormatInt(delta)}}
	return delta, s.commit(bt, fw)
}

func (s *Store) incrFloat(db uint32, key []byte, delta float64) (float64, error) {
	o, err := s.loadStringRow(db, key, true)
	if err != nil {
		return 0, err
	}

	bt := engine.NewBatch()
	if o != nil {
		_, err := o.LoadDataValue(s)
		if err != nil {
			return 0, err
		}
		v, err := ParseFloat(o.Value)
		if err != nil {
			return 0, err
		}
		delta += v
	} else {
		o = newStringRow(db, key)
		bt.Set(o.MetaKey(), o.MetaValue())
	}

	if math.IsNaN(delta) || math.IsInf(delta, 0) {
		return 0, errors.Errorf("increment would produce NaN or Infinity")
	}

	o.Value = FormatFloat(delta)
	bt.Set(o.DataKey(), o.DataValue())

	fw := &Forward{DB: db, Op: "IncrByFloat", Args: [][]byte{key, FormatFloat(delta)}}
	return delta, s.commit(bt, fw)
}

// INCR key
func (s *Store) Incr(db uint32, args [][]byte) (int64, error) {
	if len(args) != 1 {
		return 0, errArguments("len(args) = %d, expect = 1", len(args))
	}

	key := args[0]

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	return s.incrInt(db, key, 1)
}

// INCRBY key delta
func (s *Store) IncrBy(db uint32, args [][]byte) (int64, error) {
	if len(args) != 2 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	key := args[0]
	delta, err := ParseInt(args[1])
	if err != nil {
		return 0, errArguments("parse args failed - %s", err)
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	return s.incrInt(db, key, delta)
}

// DECR key
func (s *Store) Decr(db uint32, args [][]byte) (int64, error) {
	if len(args) != 1 {
		return 0, errArguments("len(args) = %d, expect = 1", len(args))
	}

	key := args[0]

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	return s.incrInt(db, key, -1)
}

// DECRBY key delta
func (s *Store) DecrBy(db uint32, args [][]byte) (int64, error) {
	if len(args) != 2 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	key := args[0]
	delta, err := ParseInt(args[1])
	if err != nil {
		return 0, errArguments("parse args failed - %s", err)
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	return s.incrInt(db, key, -delta)
}

// INCRBYFLOAT key delta
func (s *Store) IncrByFloat(db uint32, args [][]byte) (float64, error) {
	if len(args) != 2 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	key := args[0]
	delta, err := ParseFloat(args[1])
	if err != nil {
		return 0, errArguments("parse args failed - %s", err)
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	return s.incrFloat(db, key, delta)
}

// SETBIT key offset value
func (s *Store) SetBit(db uint32, args [][]byte) (int64, error) {
	if len(args) != 3 {
		return 0, errArguments("len(args) = %d, expect = 3", len(args))
	}

	key := args[0]
	offset, err := ParseUint(args[1])
	if err != nil {
		return 0, errArguments("bit offset is not an integer or out of range - %s", err)
	}
	value, err := ParseUint(args[2])
	if err != nil {
		return 0, errArguments("bit is not an integer or out of range - %s", err)
	}

	if offset > maxVarbytesLen {
		return 0, errArguments("bit offset is not an integer or out of range, offset = %d", offset)
	}

	if value != 0 && value != 1 {
		return 0, errArguments("bit is not an integer or out of range, bit = %d", value)
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadStringRow(db, key, true)
	if err != nil {
		return 0, err
	}

	bt := engine.NewBatch()
	if o != nil {
		_, err := o.LoadDataValue(s)
		if err != nil {
			return 0, err
		}
	} else {
		o = newStringRow(db, key)
		bt.Set(o.MetaKey(), o.MetaValue())
	}

	byteOffset := int(uint32(offset) >> 3)
	extra := byteOffset + 1 - len(o.Value)
	if extra > 0 {
		o.Value = append(o.Value, make([]byte, extra)...)
	}

	byteVal := o.Value[byteOffset]
	bit := 7 - uint8(uint32(offset)&0x7)
	bitVal := byteVal & (1 << bit)

	byteVal &= ^(1 << bit)
	byteVal |= (uint8(value&0x1) << bit)

	o.Value[byteOffset] = byteVal

	bt.Set(o.DataKey(), o.DataValue())

	var n int64 = 0
	if bitVal > 0 {
		n = 1
	}

	fw := &Forward{DB: db, Op: "SetBit", Args: args}
	return n, s.commit(bt, fw)
}

// SETRANGE key offset value
func (s *Store) SetRange(db uint32, args [][]byte) (int64, error) {
	if len(args) != 3 {
		return 0, errArguments("len(args) = %d, expect = 3", len(args))
	}

	key := args[0]
	offset, err := ParseUint(args[1])
	if err != nil {
		return 0, errArguments("parse args failed - %s", err)
	}
	value := args[2]

	if offset > maxVarbytesLen {
		return 0, errArguments("offset = %d", offset)
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadStringRow(db, key, true)
	if err != nil {
		return 0, err
	}

	bt := engine.NewBatch()
	if o != nil {
		_, err := o.LoadDataValue(s)
		if err != nil {
			return 0, err
		}
	} else {
		o = newStringRow(db, key)
		bt.Set(o.MetaKey(), o.MetaValue())
	}

	if n := int(offset) + len(value); n > len(o.Value) {
		o.Value = append(o.Value, make([]byte, n-len(o.Value))...)
	}

	copy(o.Value[offset:], value)
	bt.Set(o.DataKey(), o.DataValue())

	fw := &Forward{DB: db, Op: "SetRange", Args: args}
	return int64(len(o.Value)), s.commit(bt, fw)
}

// MSET key value [key value ...]
func (s *Store) MSet(db uint32, args [][]byte) error {
	if len(args) == 0 || len(args)%2 != 0 {
		return errArguments("len(args) = %d, expect != 0 && mod 2 = 0", len(args))
	}

	if err := s.acquire(); err != nil {
		return err
	}
	defer s.release()

	ms := &markSet{}
	bt := engine.NewBatch()
	for i := len(args)/2 - 1; i >= 0; i-- {
		key, value := args[i*2], args[i*2+1]
		if !ms.Has(key) {
			_, err := s.checkExistAndDelete(bt, db, key)
			if err != nil {
				return err
			}

			o := newStringRow(db, key)
			o.Value = value
			bt.Set(o.DataKey(), o.DataValue())
			bt.Set(o.MetaKey(), o.MetaValue())
			ms.Set(key)
		}
	}

	fw := &Forward{DB: db, Op: "MSet", Args: args}
	return s.commit(bt, fw)
}

// MSETNX key value [key value ...]
func (s *Store) MSetNX(db uint32, args [][]byte) (int64, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return 0, errArguments("len(args) = %d, expect != 0 && mod 2 = 0", len(args))
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	for i := 0; i < len(args); i += 2 {
		o, err := s.loadStoreRow(db, args[i], true)
		if err != nil || o != nil {
			return 0, err
		}
	}

	ms := &markSet{}
	bt := engine.NewBatch()
	for i := len(args)/2 - 1; i >= 0; i-- {
		key, value := args[i*2], args[i*2+1]
		if !ms.Has(key) {
			o := newStringRow(db, key)
			o.Value = value
			bt.Set(o.DataKey(), o.DataValue())
			bt.Set(o.MetaKey(), o.MetaValue())
			ms.Set(key)
		}
	}

	fw := &Forward{DB: db, Op: "MSet", Args: args}
	return 1, s.commit(bt, fw)
}

// MGET key [key ...]
func (s *Store) MGet(db uint32, args [][]byte) ([][]byte, error) {
	if len(args) == 0 {
		return nil, errArguments("len(args) = %d, expect != 0", len(args))
	}

	keys := args

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	values := make([][]byte, len(keys))
	for i, key := range keys {
		o, err := s.loadStringRow(db, key, false)
		if err != nil {
			return nil, err
		}
		if o != nil {
			_, err := o.LoadDataValue(s)
			if err != nil {
				return nil, err
			}

			values[i] = o.Value
		}
	}

	return values, nil
}

// GETBIT key offset
func (s *Store) GetBit(db uint32, args [][]byte) (int64, error) {
	if len(args) != 2 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	key := args[0]
	offset, err := ParseUint(args[1])
	if err != nil {
		return 0, errArguments("bit offset is not an integer or out of range - %s", err)
	}

	if offset > maxVarbytesLen {
		return 0, errArguments("bit offset is not an integer or out of range, offset=%d", offset)
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadStringRow(db, key, false)
	if err != nil || o == nil {
		return 0, err
	}

	if _, err := o.LoadDataValue(s); err != nil {
		return 0, err
	}

	byteOffset := uint32(offset) >> 3
	bit := 7 - uint8(uint32(offset)&0x7)

	if byteOffset >= uint32(len(o.Value)) {
		return 0, nil
	}

	bitVal := o.Value[byteOffset] & (1 << bit)
	if bitVal > 0 {
		return 1, nil
	}

	return 0, nil
}

// GETRANGE key beg end
func (s *Store) GetRange(db uint32, args [][]byte) ([]byte, error) {
	if len(args) != 3 {
		return nil, errArguments("len(args) = %d, expect = 3", len(args))
	}

	key := args[0]
	beg, err := ParseInt(args[1])
	if err != nil {
		return nil, errArguments("parse args failed - %s", err)
	}
	end, err := ParseInt(args[2])
	if err != nil {
		return nil, errArguments("parse args failed - %s", err)
	}

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	o, err := s.loadStringRow(db, key, false)
	if err != nil {
		return nil, err
	}

	if o != nil {
		_, err := o.LoadDataValue(s)
		if err != nil {
			return nil, err
		}

		min, max := int64(0), int64(len(o.Value))
		beg = maxIntValue(adjustIndex(beg, min, max), min)
		end = minIntValue(adjustIndex(end, min, max), max-1)
		if beg <= end {
			return o.Value[beg : end+1], nil
		}
	}

	return nil, nil
}

// STRLEN key
func (s *Store) Strlen(db uint32, args [][]byte) (int64, error) {
	if len(args) != 1 {
		return 0, errArguments("len(args) = %d, expect = 1", len(args))
	}

	key := args[0]

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadStringRow(db, key, false)
	if err != nil {
		return 0, err
	}

	if o != nil {
		_, err := o.LoadDataValue(s)
		if err != nil {
			return 0, err
		}

		return int64(len(o.Value)), nil
	}

	return 0, nil
}

// BITCOUNT key [beg end]
func (s *Store) BitCount(db uint32, args [][]byte) (int64, error) {
	if len(args) != 1 && len(args) != 3 {
		return 0, errArguments("len(args) = %d, expect = 1 || expect == 3", len(args))
	}

	key := args[0]

	var beg, end int64 = 0, -1
	if len(args) == 3 {
		var err error
		beg, err = ParseInt(args[1])
		if err != nil {
			return 0, errArguments("value is not an integer or out of range - %s", err)
		}

		end, err = ParseInt(args[2])
		if err != nil {
			return 0, errArguments("value is not an integer or out of range - %s", err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadStringRow(db, key, false)
	if err != nil {
		return 0, err
	}

	var n int64 = 0

	if o != nil {
		_, err := o.LoadDataValue(s)
		if err != nil {
			return 0, err
		}

		min, max := int64(0), int64(len(o.Value))
		beg = maxIntValue(adjustIndex(beg, min, max), min)
		end = minIntValue(adjustIndex(end, min, max), max-1)
		if beg > end {
			return 0, nil
		}

		o.Value = o.Value[beg : end+1]

		pos := 0
		for ; pos+4 <= len(o.Value); pos = pos + 4 {
			n += int64(numberBitCount(binary.BigEndian.Uint32(o.Value[pos : pos+4])))
		}

		for ; pos < len(o.Value); pos++ {
			n += int64(bitsInByte[o.Value[pos]])
		}
	}

	return n, nil
}

// BITOP op destkey key [key ...]
func (s *Store) BitOp(db uint32, args [][]byte) (int64, error) {
	if len(args) < 3 {
		return 0, errArguments("len(args) = %d, expect >= 3", len(args))
	}

	op := args[0]
	destKey := args[1]
	srcKeys := args[2:]

	if string(op) == BitNot && len(srcKeys) > 1 {
		return 0, errArguments("BITOP NOT must be called with a single source key, len(srcKeys)=%d", len(srcKeys))
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	var value []byte
	o, err := s.loadStringRow(db, srcKeys[0], true)
	if err != nil {
		return 0, err
	}

	if o != nil {
		_, err := o.LoadDataValue(s)
		if err != nil {
			return 0, err
		}

		if string(op) == BitNot {
			for i := 0; i < len(o.Value); i++ {
				o.Value[i] = ^o.Value[i]
			}
		}

		value = o.Value
	}

	for i := 1; i < len(srcKeys); i++ {
		ro, err := s.loadStringRow(db, srcKeys[i], true)
		if err != nil {
			return 0, err
		}

		if ro != nil {
			_, err = ro.LoadDataValue(s)
			if err != nil {
				return 0, err
			}
		} else {
			ro = newStringRow(db, srcKeys[i])
		}

		if len(value) < len(ro.Value) {
			value, ro.Value = ro.Value, value
		}

		for j := 0; j < len(ro.Value); j++ {
			switch string(op) {
			case BitAND:
				value[j] &= ro.Value[j]
			case BitOR:
				value[j] |= ro.Value[j]
			case BitXOR:
				value[j] ^= ro.Value[j]
			default:
				return 0, fmt.Errorf("invalid op type: %s", op)
			}
		}

		for j := len(ro.Value); j < len(value); j++ {
			switch string(op) {
			case BitAND:
				value[j] &= 0
			case BitOR:
				value[j] |= 0
			case BitXOR:
				value[j] ^= 0
			}
		}
	}

	bt := engine.NewBatch()

	_, err = s.checkExistAndDelete(bt, db, destKey)
	if err != nil {
		return 0, err
	}

	no := newStringRow(db, destKey)
	no.Value = value
	bt.Set(no.DataKey(), no.DataValue())
	bt.Set(no.MetaKey(), no.MetaValue())

	fw := &Forward{DB: db, Op: "BitOp", Args: args}
	return int64(len(no.Value)), s.commit(bt, fw)
}
