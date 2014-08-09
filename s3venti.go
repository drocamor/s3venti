// A venti that stores stuff in S3
// Keeps an index of the blocks in a kv store
// Caches blocks
// Based off the grande example in the govt project

package main

import (
	"code.google.com/p/govt/vt"
	"code.google.com/p/govt/vt/vtsrv"
	"crypto/sha1"
	"flag"

	"github.com/cznic/kv"
	"hash"
	"log"
	"os"
	"os/signal"
	"os/user"
)

type S3Venti struct {
	vtsrv.Srv
	schan chan hash.Hash
	db    *kv.DB
}

var addr = flag.String("addr", ":17034", "network address")
var debug = flag.Int("debug", 0, "print debug messages")

func eqscore(s1, s2 vt.Score) bool {
	for i := 0; i < vt.Scoresize; i++ {
		if s1[i] != s2[i] {
			return false
		}
	}

	return true
}

func (srv *S3Venti) init() {
	srv.schan = make(chan hash.Hash, 32)
}

func (srv *S3Venti) calcScore(data []byte) (ret vt.Score) {
	var s1 hash.Hash

	select {
	default:
		s1 = sha1.New()
	case s1 = <-srv.schan:
		s1.Reset()
	}

	s1.Write(data)
	ret = s1.Sum(nil)
	select {
	case srv.schan <- s1:
		break
	default:
	}
	return
}

func (srv *S3Venti) Hello(req *vtsrv.Req) {
	req.RespondHello("anonymous", 0, 0)
}

func (srv *S3Venti) Read(req *vtsrv.Req) {

	b, err := srv.db.Get(nil, req.Tc.Score)

	if err != nil {
		req.RespondError(err.Error())
		return
	}

	req.RespondRead(b)
}

func (srv *S3Venti) Write(req *vtsrv.Req) {

	s := srv.calcScore(req.Tc.Data)
	err := srv.db.BeginTransaction()
	if err != nil {
		req.RespondError(err.Error())
		return
	}

	err = srv.db.Set(s, req.Tc.Data)
	if err != nil {
		req.RespondError(err.Error())
		return
	}

	err = srv.db.Commit()
	if err != nil {
		req.RespondError(err.Error())
		return
	}

	req.RespondWrite(s)
}

func (srv *S3Venti) cleanup() {
	log.Println("Trying to close DB...")
	err := srv.db.Close()

	if err != nil {
		log.Fatal("Couldn't close DB:", err)
	}

	log.Println("DB is closed")
}

func main() {
	flag.Parse()

	srv := new(S3Venti)
	srv.init()

	usr, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}

	ventiFile := usr.HomeDir + "/.s3venti/scores.db"

	db, err := kv.Open(ventiFile, &kv.Options{})

	if err != nil {
		db, err = kv.Create(ventiFile, &kv.Options{})
		if err != nil {
			log.Fatal("Cannot open or create database:", err)
		}
	}

	srv.db = db

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for _ = range c {
			// sig is a ^C, handle it
			srv.cleanup()
			os.Exit(1)
		}
	}()

	srv.Debuglevel = *debug
	srv.Start(srv)
	srv.StartStatsServer()
	vtsrv.StartListener("tcp", *addr, &srv.Srv)

}
