// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pools

import (
	"errors"
	"testing"
	"time"

	"github.com/reborndb/go/atomic2"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testPoolsSuite{})

type testPoolsSuite struct {
}

var lastId, count atomic2.Int64

type TestResource struct {
	num    int64
	closed bool
}

func (tr *TestResource) Close() {
	if !tr.closed {
		count.Add(-1)
		tr.closed = true
	}
}

func (tr *TestResource) IsClosed() bool {
	return tr.closed
}

func PoolFactory() (Resource, error) {
	count.Add(1)
	return &TestResource{lastId.Add(1), false}, nil
}

func FailFactory() (Resource, error) {
	return nil, errors.New("Failed")
}

func SlowFailFactory() (Resource, error) {
	time.Sleep(10 * time.Nanosecond)
	return nil, errors.New("Failed")
}

func (s *testPoolsSuite) TestOpen(c *C) {
	lastId.Set(0)
	count.Set(0)
	p := NewResourcePool(PoolFactory, 6, 6, time.Second)
	p.SetCapacity(5)
	var resources [10]Resource

	// Test Get
	for i := 0; i < 5; i++ {
		r, err := p.Get()
		resources[i] = r
		c.Assert(err, IsNil)

		_, available, _, waitCount, waitTime, _ := p.Stats()
		c.Assert(available, Equals, int64(5-i-1))
		c.Assert(waitCount, Equals, int64(0))
		c.Assert(waitTime, Equals, time.Duration(0))
		c.Assert(lastId.Get(), Equals, int64(i+1))
		c.Assert(count.Get(), Equals, int64(i+1))
	}

	// Test TryGet
	r, err := p.TryGet()
	c.Assert(err, IsNil)
	c.Assert(r, IsNil)

	for i := 0; i < 5; i++ {
		p.Put(resources[i])
		_, available, _, _, _, _ := p.Stats()
		c.Assert(available, Equals, int64(i+1))
	}

	for i := 0; i < 5; i++ {
		r, err := p.TryGet()
		resources[i] = r
		c.Assert(err, IsNil)
		c.Assert(r, NotNil)
		c.Assert(lastId.Get(), Equals, int64(5))
		c.Assert(count.Get(), Equals, int64(5))
	}

	// Test that Get waits
	ch := make(chan bool)
	go func() {
		for i := 0; i < 5; i++ {
			r, err := p.Get()
			c.Assert(err, IsNil)

			resources[i] = r
		}
		for i := 0; i < 5; i++ {
			p.Put(resources[i])
		}
		ch <- true
	}()
	for i := 0; i < 5; i++ {
		// Sleep to ensure the goroutine waits
		time.Sleep(10 * time.Nanosecond)
		p.Put(resources[i])
	}
	<-ch
	_, _, _, waitCount, waitTime, _ := p.Stats()
	c.Assert(waitCount, Equals, int64(5))
	c.Assert(waitTime, Not(Equals), time.Duration(0))
	c.Assert(lastId.Get(), Equals, int64(5))

	// Test Close resource
	r, err = p.Get()
	c.Assert(err, IsNil)

	r.Close()
	p.Put(nil)
	c.Assert(count.Get(), Equals, int64(4))

	for i := 0; i < 5; i++ {
		r, err := p.Get()
		c.Assert(err, IsNil)

		resources[i] = r
	}

	for i := 0; i < 5; i++ {
		p.Put(resources[i])
	}

	c.Assert(count.Get(), Equals, int64(5))
	c.Assert(lastId.Get(), Equals, int64(6))

	// SetCapacity
	p.SetCapacity(3)
	c.Assert(count.Get(), Equals, int64(3))
	c.Assert(lastId.Get(), Equals, int64(6))

	capacity, available, _, _, _, _ := p.Stats()
	c.Assert(capacity, Equals, int64(3))
	c.Assert(available, Equals, int64(3))

	p.SetCapacity(6)
	capacity, available, _, _, _, _ = p.Stats()
	c.Assert(capacity, Equals, int64(6))
	c.Assert(available, Equals, int64(6))

	for i := 0; i < 6; i++ {
		r, err := p.Get()
		c.Assert(err, IsNil)

		resources[i] = r
	}

	for i := 0; i < 6; i++ {
		p.Put(resources[i])
	}

	c.Assert(count.Get(), Equals, int64(6))
	c.Assert(lastId.Get(), Equals, int64(9))

	// Close
	p.Close()
	capacity, available, _, _, _, _ = p.Stats()
	c.Assert(capacity, Equals, int64(0))
	c.Assert(available, Equals, int64(0))
	c.Assert(count.Get(), Equals, int64(0))
}

func (s *testPoolsSuite) TestShrinking(c *C) {
	lastId.Set(0)
	count.Set(0)
	p := NewResourcePool(PoolFactory, 5, 5, time.Second)
	var resources [10]Resource
	// Leave one empty slot in the pool
	for i := 0; i < 4; i++ {
		r, err := p.Get()
		c.Assert(err, IsNil)

		resources[i] = r
	}
	go p.SetCapacity(3)
	time.Sleep(10 * time.Nanosecond)
	stats := p.StatsJSON()
	expected := `{"Capacity": 3, "Available": 0, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	c.Assert(stats, Equals, expected)

	// TryGet is allowed when shrinking
	r, err := p.TryGet()
	c.Assert(err, IsNil)
	c.Assert(r, IsNil)

	// Get is allowed when shrinking, but it will wait
	getdone := make(chan bool)
	go func() {
		r, err := p.Get()
		c.Assert(err, IsNil)

		p.Put(r)
		getdone <- true
	}()

	// Put is allowed when shrinking. It's necessary.
	for i := 0; i < 4; i++ {
		p.Put(resources[i])
	}

	// Wait for Get test to complete
	<-getdone
	stats = p.StatsJSON()
	expected = `{"Capacity": 3, "Available": 3, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	c.Assert(stats, Equals, expected)
	c.Assert(count.Get(), Equals, int64(3))

	// Ensure no deadlock if SetCapacity is called after we start
	// waiting for a resource
	for i := 0; i < 3; i++ {
		resources[i], err = p.Get()
		c.Assert(err, IsNil)
	}

	// This will wait because pool is empty
	go func() {
		r, err := p.Get()
		c.Assert(err, IsNil)

		p.Put(r)
		getdone <- true
	}()
	time.Sleep(10 * time.Nanosecond)

	// This will wait till we Put
	go p.SetCapacity(2)
	time.Sleep(10 * time.Nanosecond)

	// This should not hang
	for i := 0; i < 3; i++ {
		p.Put(resources[i])
	}
	<-getdone

	capacity, available, _, _, _, _ := p.Stats()
	c.Assert(capacity, Equals, int64(2))
	c.Assert(available, Equals, int64(2))
	c.Assert(count.Get(), Equals, int64(2))

	// Test race condition of SetCapacity with itself
	p.SetCapacity(3)
	for i := 0; i < 3; i++ {
		resources[i], err = p.Get()
		c.Assert(err, IsNil)
	}

	// This will wait because pool is empty
	go func() {
		r, err := p.Get()
		c.Assert(err, IsNil)

		p.Put(r)
		getdone <- true
	}()
	time.Sleep(10 * time.Nanosecond)

	// This will wait till we Put
	go p.SetCapacity(2)
	time.Sleep(10 * time.Nanosecond)
	go p.SetCapacity(4)
	time.Sleep(10 * time.Nanosecond)

	// This should not hang
	for i := 0; i < 3; i++ {
		p.Put(resources[i])
	}
	<-getdone

	err = p.SetCapacity(-1)
	c.Assert(err, NotNil)

	err = p.SetCapacity(255555)
	c.Assert(err, NotNil)

	capacity, available, _, _, _, _ = p.Stats()
	c.Assert(capacity, Equals, int64(4))
	c.Assert(available, Equals, int64(4))
}

func (s *testPoolsSuite) TestClosing(c *C) {
	lastId.Set(0)
	count.Set(0)
	p := NewResourcePool(PoolFactory, 5, 5, time.Second)
	var resources [10]Resource
	for i := 0; i < 5; i++ {
		r, err := p.Get()
		c.Assert(err, IsNil)

		resources[i] = r
	}
	ch := make(chan bool)
	go func() {
		p.Close()
		ch <- true
	}()

	// Wait for goroutine to call Close
	time.Sleep(10 * time.Nanosecond)
	stats := p.StatsJSON()
	expected := `{"Capacity": 0, "Available": 0, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	c.Assert(stats, Equals, expected)

	// Put is allowed when closing
	for i := 0; i < 5; i++ {
		p.Put(resources[i])
	}

	// Wait for Close to return
	<-ch

	// SetCapacity must be ignored after Close
	err := p.SetCapacity(1)
	c.Assert(err, NotNil)

	stats = p.StatsJSON()
	expected = `{"Capacity": 0, "Available": 0, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	c.Assert(stats, Equals, expected)
	c.Assert(lastId.Get(), Equals, int64(5))
	c.Assert(count.Get(), Equals, int64(0))
}

func (s *testPoolsSuite) TestIdleTimeout(c *C) {
	lastId.Set(0)
	count.Set(0)
	p := NewResourcePool(PoolFactory, 1, 1, 10*time.Nanosecond)
	defer p.Close()

	r, err := p.Get()
	c.Assert(err, IsNil)

	p.Put(r)
	c.Assert(lastId.Get(), Equals, int64(1))
	c.Assert(count.Get(), Equals, int64(1))

	time.Sleep(20 * time.Nanosecond)
	r, err = p.Get()
	c.Assert(err, IsNil)
	c.Assert(lastId.Get(), Equals, int64(2))
	c.Assert(count.Get(), Equals, int64(1))

	p.Put(r)
}

func (s *testPoolsSuite) TestCreateFail(c *C) {
	lastId.Set(0)
	count.Set(0)
	p := NewResourcePool(FailFactory, 5, 5, time.Second)
	defer p.Close()

	_, err := p.Get()
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Failed")

	stats := p.StatsJSON()
	expected := `{"Capacity": 5, "Available": 5, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	c.Assert(stats, Equals, expected)
}

func (s *testPoolsSuite) TestSlowCreateFail(c *C) {
	lastId.Set(0)
	count.Set(0)
	p := NewResourcePool(SlowFailFactory, 2, 2, time.Second)
	defer p.Close()

	ch := make(chan bool)
	// The third Get should not wait indefinitely
	for i := 0; i < 3; i++ {
		go func() {
			p.Get()
			ch <- true
		}()
	}
	for i := 0; i < 3; i++ {
		<-ch
	}
	_, available, _, _, _, _ := p.Stats()
	c.Assert(available, Equals, int64(2))
}
