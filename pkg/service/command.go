package service

import (
	"fmt"
	"strings"

	redis "github.com/reborndb/go/redis/resp"
)

type CommandFunc func(c *conn, args [][]byte) (redis.Resp, error)

var globalCommands = make(map[string]CommandFunc)

func Register(name string, f CommandFunc) {
	funcName := strings.ToLower(name)
	if _, ok := globalCommands[funcName]; ok {
		panic(fmt.Sprintf("%s has been registered", name))
	}

	globalCommands[funcName] = f
}
