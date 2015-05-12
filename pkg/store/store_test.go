// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"os"
	"testing"
	"time"

	"github.com/reborndb/go/log"
	"github.com/reborndb/go/testing/assert"
	"github.com/reborndb/qdb/pkg/engine/rocksdb"
)

var (
	testStore *Store
)

func reinit() {
	if testStore != nil {
		testStore.Close()
		testStore = nil
	}
	const path = "/tmp/test_qdb/store/testdb-rocksdb"
	if err := os.RemoveAll(path); err != nil {
		log.PanicErrorf(err, "remove '%s' failed", path)
	} else {
		conf := rocksdb.NewDefaultConfig()
		if testdb, err := rocksdb.Open(path, conf, false); err != nil {
			log.PanicError(err, "open rocksdb failed")
		} else {
			testStore = New(testdb)
		}
	}
}

func init() {
	reinit()
	log.SetFlags(log.Flags() | log.Lshortfile)
}

func checkerror(t *testing.T, err error, exp bool) {
	if err != nil || !exp {
		reinit()
	}
	assert.ErrorIsNil(t, err)
	assert.Must(t, exp)
}

func checkcompact(t *testing.T) {
	err := testStore.CompactAll()
	checkerror(t, err, true)
}

func checkempty(t *testing.T) {
	it := testStore.getIterator()
	it.SeekToFirst()
	empty, err := !it.Valid(), it.Error()
	testStore.putIterator(it)
	checkerror(t, err, empty)
}

func sleepms(n int) {
	time.Sleep(time.Millisecond * time.Duration(n))
}
