package main

import (
	"flag"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/juju/errors"
	"github.com/mgxian/go-mysql-sync/river"
	"gopkg.in/birkirb/loggers.v1/log"
)

var configFile = flag.String("config", "./etc/river.toml", "go-mysql-sync config file")
var src_my_addr = flag.String("src_my_addr", "", "source MySQL addr")
var src_my_user = flag.String("src_my_user", "", "source MySQL user")
var src_my_pass = flag.String("src_my_pass", "", "source MySQL password")
var dest_my_addr = flag.String("dest_my_addr", "", "destination MySQL addr")
var dest_my_user = flag.String("dest_my_user", "", "destination MySQL user")
var dest_my_pass = flag.String("dest_my_pass", "", "destination MySQL password")
var data_dir = flag.String("data_dir", "", "path for go-mysql-sync to save data")
var server_id = flag.Int("server_id", 0, "MySQL server id, as a pseudo slave")
var flavor = flag.String("flavor", "", "flavor: mysql or mariadb")
var execution = flag.String("exec", "", "mysqldump execution path")
var logLevel = flag.String("log_level", "info", "log level")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	level := log.ParseLevel(*logLevel)
	log.SetLevel(level)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	cfg, err := river.NewConfigWithFile(*configFile)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	if len(*src_my_addr) > 0 {
		cfg.SrcMyAddr = *src_my_addr
	}

	if len(*src_my_user) > 0 {
		cfg.SrcMyUser = *src_my_user
	}

	if len(*src_my_pass) > 0 {
		cfg.SrcMyPassword = *src_my_pass
	}

	if len(*dest_my_addr) > 0 {
		cfg.DestMyAddr = *dest_my_addr
	}

	if len(*dest_my_user) > 0 {
		cfg.DestMyUser = *dest_my_user
	}

	if len(*dest_my_pass) > 0 {
		cfg.DestMyPassword = *dest_my_pass
	}

	if *server_id > 0 {
		cfg.ServerID = uint32(*server_id)
	}

	if len(*data_dir) > 0 {
		cfg.DataDir = *data_dir
	}

	if len(*flavor) > 0 {
		cfg.Flavor = *flavor
	}

	if len(*execution) > 0 {
		cfg.DumpExec = *execution
	}

	r, err := river.NewRiver(cfg)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	done := make(chan struct{}, 1)
	go func() {
		r.Run()
		done <- struct{}{}
	}()

	select {
	case n := <-sc:
		log.Infof("receive signal %v, closing", n)
	case <-r.Ctx().Done():
		log.Infof("context is done with %v, closing", r.Ctx().Err())
	}

	r.Close()
	<-done
}
