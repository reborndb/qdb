// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package binlog

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
func (b *Binlog) RegBeforeCommitHandler(h ForwardHandler) {
	if err := b.acquire(); err != nil {
		return
	}
	defer b.release()

	b.beforeCommitHandlers = append(b.beforeCommitHandlers, h)
}

// Register the handler that will be called after db storage committed
func (b *Binlog) RegAfterCommitHandler(h ForwardHandler) {
	if err := b.acquire(); err != nil {
		return
	}
	defer b.release()

	b.afterCommitHandlers = append(b.afterCommitHandlers, h)
}

func (b *Binlog) travelBeforeCommitHandlers(f *Forward) {
	for _, h := range b.beforeCommitHandlers {
		if err := h(f); err != nil {
			log.WarnErrorf(err, "handle WillCommitHandler err")
		}
	}
}

func (b *Binlog) travelAfterCommitHandlers(f *Forward) {
	for _, h := range b.afterCommitHandlers {
		if err := h(f); err != nil {
			log.WarnErrorf(err, "handle DidCommitHandler err")
		}
	}
}
