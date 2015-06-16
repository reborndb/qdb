// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package handler

import (
	"github.com/reborndb/go/errors"
	"github.com/reborndb/go/redis/resp"
)

type Server struct {
	t HandlerTable
}

func NewServer(o interface{}) (*Server, error) {
	t, err := NewHandlerTable(o)
	if err != nil {
		return nil, err
	}
	return &Server{t}, nil
}

func NewServerWithTable(t HandlerTable) (*Server, error) {
	if t == nil {
		return nil, errors.New("handler table is nil")
	}
	return &Server{t}, nil
}

func MustServer(o interface{}) *Server {
	return &Server{MustHandlerTable(o)}
}

func (s *Server) Dispatch(arg0 interface{}, respArg resp.Resp) (resp.Resp, error) {
	if cmd, args, err := resp.ParseArgs(respArg); err != nil {
		return nil, err
	} else if f := s.t[cmd]; f == nil {
		return nil, errors.Errorf("unknown command '%s'", cmd)
	} else {
		return f(arg0, args...)
	}
}
