// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"bytes"
	"math"
	"sort"

	. "gopkg.in/check.v1"
)

func testEncodeFloat(f float64) []byte {
	b := NewBufWriter(nil)
	b.WriteFloat64(f)
	return b.Bytes()
}

func testDecodeFloat(b []byte) float64 {
	r := NewBufReader(b)
	f, _ := r.ReadFloat64()
	return f
}

type bytesSlice [][]byte

func (s bytesSlice) Len() int {
	return len(s)
}

func (s bytesSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s bytesSlice) Less(i, j int) bool {
	return bytes.Compare(s[i], s[j]) < 0
}

func testFloatLexSort(c *C, src []float64, check []float64) {
	ay := make(bytesSlice, 0, len(src))

	for _, f := range src {
		ay = append(ay, testEncodeFloat(f))
	}

	sort.Sort(ay)

	for i, b := range ay {
		f := testDecodeFloat(b)
		c.Assert(f, Equals, check[i])
	}
}

func testFloatCodec(c *C, f float64) {
	c.Assert(testDecodeFloat(testEncodeFloat(f)), Equals, f)
}

func (s *testStoreSuite) TestFloatCodec(c *C) {
	testFloatCodec(c, float64(1.0))
	testFloatCodec(c, float64(-1.0))
	testFloatCodec(c, float64(0))
	testFloatCodec(c, float64(-3.14))
	testFloatCodec(c, float64(3.14))
	testFloatCodec(c, math.MaxFloat64)
	testFloatCodec(c, -math.MaxFloat64)
	testFloatCodec(c, math.SmallestNonzeroFloat64)
	testFloatCodec(c, -math.SmallestNonzeroFloat64)
}

func (s *testStoreSuite) TestFloatLexCodec(c *C) {
	testFloatLexSort(c,
		[]float64{1, -1},
		[]float64{-1, 1})

	testFloatLexSort(c,
		[]float64{-2, -3.14, 3.14},
		[]float64{-3.14, -2, 3.14})

	testFloatLexSort(c,
		[]float64{-2.0, -3.0, 1},
		[]float64{-3.0, -2.0, 1})

	testFloatLexSort(c,
		[]float64{1.0, 0, -10.0, math.MaxFloat64, -math.MaxFloat64},
		[]float64{-math.MaxFloat64, -10.0, 0, 1.0, math.MaxFloat64})
}