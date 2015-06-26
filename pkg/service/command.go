// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	"strings"

	"github.com/ngaut/log"
	redis "github.com/reborndb/go/redis/resp"
)

type command struct {
	name string
	f    CommandFunc
	flag CommandFlag
}

var globalCommands = make(map[string]*command)

func register(name string, f CommandFunc, flag CommandFlag) {
	funcName := strings.ToLower(name)
	if _, ok := globalCommands[funcName]; ok {
		log.Fatalf("%s has been registered", name)
	}

	globalCommands[funcName] = &command{name, f, flag}
}

// return a common RESP
type CommandFunc func(s Session, args [][]byte) (redis.Resp, error)

// return int64 RESP, or error RESP if err is not nil
type CommandIntFunc func(s Session, args [][]byte) (int64, error)

// return bulk string RESP, or error RESP if err is not nil
type CommandBulkStringFunc func(s Session, args [][]byte) ([]byte, error)

// return simple string RESP, or error RESP if err is not nil
type CommandSimpleStringFunc func(s Session, args [][]byte) (string, error)

// return array RESP, or error RESP if err is not nil
type CommandArrayFunc func(s Session, args [][]byte) ([][]byte, error)

// return OK simple string RESP if error is nil, or error RESP if err is not nil
type CommandOKFunc func(s Session, args [][]byte) error

type CommandFlag uint32

const (
	ReadCommandFlag CommandFlag = 1 << iota
	WriteCommandFlag
)

func Register(name string, f CommandFunc, flag CommandFlag) {
	register(name, f, flag)
}

func RegisterIntReply(name string, f CommandIntFunc, flag CommandFlag) {
	v := func(s Session, args [][]byte) (redis.Resp, error) {
		r, err := f(s, args)
		if err != nil {
			return toRespError(err)
		}
		return redis.NewInt(r), nil
	}

	register(name, v, flag)
}

func RegisterBulkReply(name string, f CommandBulkStringFunc, flag CommandFlag) {
	v := func(s Session, args [][]byte) (redis.Resp, error) {
		r, err := f(s, args)
		if err != nil {
			return toRespError(err)
		}
		return redis.NewBulkBytes(r), nil
	}

	register(name, v, flag)
}

func RegisterStringReply(name string, f CommandSimpleStringFunc, flag CommandFlag) {
	v := func(s Session, args [][]byte) (redis.Resp, error) {
		r, err := f(s, args)
		if err != nil {
			return toRespError(err)
		}
		return redis.NewString(r), nil
	}

	register(name, v, flag)
}

func RegisterArrayReply(name string, f CommandArrayFunc, flag CommandFlag) {
	v := func(s Session, args [][]byte) (redis.Resp, error) {
		r, err := f(s, args)
		if err != nil {
			return toRespError(err)
		}
		ay := redis.NewArray()
		for _, b := range r {
			ay.AppendBulkBytes(b)
		}
		return ay, nil
	}

	register(name, v, flag)
}

func RegisterOKReply(name string, f CommandOKFunc, flag CommandFlag) {
	v := func(s Session, args [][]byte) (redis.Resp, error) {
		err := f(s, args)
		if err != nil {
			return toRespError(err)
		}
		return redis.NewString("OK"), nil

	}

	register(name, v, flag)
}
