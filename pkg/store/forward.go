// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"github.com/reborndb/go/log"
)

type Forward struct {
	DB   uint32
	Op   string
	Args []interface{}
}

type ForwardHandler func(f *Forward) error

// Register the handler that will be called before db storage commit
func (b *Binlog) RegPreCommitHandler(h ForwardHandler) {
	if err := b.acquire(); err != nil {
		return
	}
	defer b.release()

	b.preCommitHandlers = append(b.preCommitHandlers, h)
}

// Register the handler that will be called after db storage committed
func (b *Binlog) RegPostCommitHandler(h ForwardHandler) {
	if err := b.acquire(); err != nil {
		return
	}
	defer b.release()

	b.postCommitHandlers = append(b.postCommitHandlers, h)
}

func (b *Binlog) travelPreCommitHandlers(f *Forward) {
	for _, h := range b.preCommitHandlers {
		if err := h(f); err != nil {
			log.WarnErrorf(err, "handle WillCommitHandler err")
		}
	}
}

func (b *Binlog) travelPostCommitHandlers(f *Forward) {
	for _, h := range b.postCommitHandlers {
		if err := h(f); err != nil {
			log.WarnErrorf(err, "handle DidCommitHandler err")
		}
	}
}
