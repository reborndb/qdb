// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

// +build all leveldb

package leveldb

import (
	"fmt"

	"github.com/reborndb/qdb/pkg/store"
)

type driver struct {
}

func (d driver) Open(path string, conf interface{}, repair bool) (store.Database, error) {
	cfg, ok := conf.(*Config)
	if !ok {
		return nil, fmt.Errorf("conf type is not leveldb config, invalid")
	}

	return Open(path, cfg, repair)
}

func init() {
	store.Register("leveldb", driver{})
}
