// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"bytes"
	"fmt"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/docopt/docopt-go"
	"github.com/reborndb/go/errors"
	"github.com/reborndb/go/log"
	"github.com/reborndb/qdb/pkg/engine"
	"github.com/reborndb/qdb/pkg/engine/goleveldb"
	"github.com/reborndb/qdb/pkg/engine/leveldb"
	"github.com/reborndb/qdb/pkg/engine/rocksdb"
	"github.com/reborndb/qdb/pkg/service"
	"github.com/reborndb/qdb/pkg/store"
)

var args struct {
	config string
	create bool
	repair bool
}

func init() {
	log.SetLevel(log.LEVEL_INFO)
	log.SetTrace(log.LEVEL_WARN)
	log.SetFlags(log.Flags() | log.Lshortfile)
}

type Config struct {
	DBType string `toml:"dbtype"`
	DBPath string `toml:"dbpath"`

	Service   *service.Config   `toml:"service"`
	LevelDB   *leveldb.Config   `toml:"leveldb"`
	RocksDB   *rocksdb.Config   `toml:"rocksdb"`
	GoLevelDB *goleveldb.Config `toml:"goleveldb"`
}

func (c *Config) LoadFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

func (c *Config) String() string {
	var b bytes.Buffer
	e := toml.NewEncoder(&b)
	e.Indent = "    "
	e.Encode(c)
	return b.String()
}

func setStringFromOpt(dest *string, d map[string]interface{}, key string) {
	if s, ok := d[key].(string); ok && len(s) != 0 {
		*dest = s
	}
}

func setIntFromOpt(dest *int, d map[string]interface{}, key string) {
	if s, ok := d[key].(string); ok && len(s) != 0 {
		if n, err := strconv.Atoi(s); err != nil {
			log.PanicErrorf(err, "parse %s failed", key)
		} else {
			*dest = n
		}
	}
}

func main() {
	usage := `
Usage:
    qdb-server [options]

Options:
    -L logfile                        log file path, if empty, use stdout
    -n N, --ncpu=N                    set runtime.GOMAXPROCS to N
    -c CONF, --config=CONF            specify the config file
    --repair                          repair database
    --dbtype=TYPE                     dtabase type, like rocksdb, leveldb, goleveldb	
    --dbpath=PATH                     database store path						
    --addr=ADDR                       service listening address	
    --auth=AUTH                       service auth
    --pidfile=FILE                    service pid file 
    --conn_timeout=N                  connection timeout after N seconds
    --dump_path=PATH                  path saving snapshot rdb file
    --sync_file_path=PATH             path saving replication syncing data
    --sync_file_size=SIZE             maximum file(bytes) size for replication syncing 
    --sync_buff_size=SIZE             maximum memory buffer size(bytes) for replication syncing
    --repl_backlog_file_path=PATH     path saving replication backlog data, if empty, use memory instead
    --repl_backlog_size=SIZE          maximum backlog size(bytes)
    --repl_ping_slave_period=N        Master pings slave in an interval(seconds) when replication
    --master_auth=MASTERAUTH          Master auth for replication
`
	d, err := docopt.Parse(usage, nil, true, "", false)
	if err != nil {
		log.PanicErrorf(err, "parse arguments failed")
	}

	if s, ok := d["-L"].(string); ok && len(s) > 0 {
		log.StdLog = log.MustFileLog(s)
	}

	if s, ok := d["--ncpu"].(string); ok && len(s) != 0 {
		if n, err := strconv.ParseInt(s, 10, 64); err != nil {
			log.PanicErrorf(err, "parse --ncpu failed")
		} else if n <= 0 || n > 64 {
			log.Panicf("parse --ncpu = %d, only accept [1,64]", n)
		} else {
			runtime.GOMAXPROCS(int(n))
		}
	}

	args.config, _ = d["--config"].(string)
	args.repair, _ = d["--repair"].(bool)

	conf := &Config{
		DBType:    "goleveldb",
		DBPath:    "./var/testdb-goleveldb",
		LevelDB:   leveldb.NewDefaultConfig(),
		RocksDB:   rocksdb.NewDefaultConfig(),
		GoLevelDB: goleveldb.NewDefaultConfig(),
		Service:   service.NewDefaultConfig(),
	}

	if args.config != "" {
		if err := conf.LoadFromFile(args.config); err != nil {
			log.PanicErrorf(err, "load config failed")
		}
	}

	setStringFromOpt(&conf.DBType, d, "--dbtype")
	setStringFromOpt(&conf.DBPath, d, "--dbpath")

	setStringFromOpt(&conf.Service.Listen, d, "--addr")
	setStringFromOpt(&conf.Service.PidFile, d, "--pidfile")
	setStringFromOpt(&conf.Service.Auth, d, "--auth")
	setIntFromOpt(&conf.Service.ConnTimeout, d, "--conn_timeout")
	setStringFromOpt(&conf.Service.DumpPath, d, "--dump_path")
	setStringFromOpt(&conf.Service.SyncFilePath, d, "--sync_file_path")
	setIntFromOpt(&conf.Service.SyncFileSize, d, "--sync_file_size")
	setIntFromOpt(&conf.Service.SyncBuffSize, d, "--sync_buff_size")
	setStringFromOpt(&conf.Service.ReplBacklogFilePath, d, "--repl_backlog_file_path")
	setIntFromOpt(&conf.Service.ReplBacklogSize, d, "--repl_backlog_size")
	setIntFromOpt(&conf.Service.ReplPingSlavePeriod, d, "--repl_ping_slave_period")
	setStringFromOpt(&conf.Service.MasterAuth, d, "--master_auth")

	log.Infof("load config\n%s\n\n", conf)

	var db engine.Database
	var dbConf interface{}
	switch t := strings.ToLower(conf.DBType); t {
	default:
		log.Panicf("unknown db type = '%s'", conf.DBType)
	case "leveldb":
		dbConf = conf.LevelDB
	case "rocksdb":
		dbConf = conf.RocksDB
	case "goleveldb":
		dbConf = conf.GoLevelDB
	}

	db, err = engine.Open(conf.DBType, conf.DBPath, dbConf, args.repair)

	if err != nil {
		log.PanicErrorf(err, "open database failed")
	}

	dbStore := store.New(db)

	if args.repair {
		return
	}

	server, err := service.NewServer(conf.Service, dbStore)
	if err != nil {
		log.PanicErrorf(err, "create server failed")
	}

	// create pid file
	createPidFile(conf.Service.PidFile)

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, os.Interrupt, os.Kill)

	go func(s *service.Server) {
		for _ = range c {
			log.Infof("interrupt and shutdown")

			// close server
			s.Close()

			// shutdown gracefully, remove pidfile
			os.Remove(conf.Service.PidFile)

			os.Exit(0)
		}
	}(server)

	if err := server.Serve(); err != nil {
		log.ErrorErrorf(err, "service failed")
	}

	server.Close()
}

func createPidFile(name string) {
	os.MkdirAll(path.Dir(name), 0755)
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.PanicErrorf(err, "create pid file %s err, panic", name)
	}
	defer f.Close()

	if _, err = f.WriteString(fmt.Sprintf("%d", os.Getpid())); err != nil {
		log.PanicErrorf(err, "write pid into pid file %s err, panic", name)
	}
}
