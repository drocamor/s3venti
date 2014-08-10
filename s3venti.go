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
	"hash"
	"log"
	"os"
	"os/signal"
	"os/user"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/cznic/kv"
)

type S3Venti struct {
	bucket *s3.Bucket
	db     *kv.DB
	vtsrv.Srv
	schan chan hash.Hash
}

var addr = flag.String("addr", ":17034", "network address")
var debug = flag.Int("debug", 0, "print debug messages")
var bucketName = flag.String("bucket", "s3venti", "s3 bucket")

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

	auth, err := aws.EnvAuth()
	if err != nil {
		panic(err.Error())
	}

	s := s3.New(auth, aws.USEast)
	srv.bucket = s.Bucket(*bucketName)
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

	// Try to get the block from the cache
	b, err := srv.db.Get(nil, req.Tc.Score)
	if err != nil {
		req.RespondError(err.Error())
		return
	}

	if b == nil {
		// Not in the cache, try S3
		b, s3err := srv.bucket.Get(req.Tc.Score.String())

		if s3err != nil {
			// Not in S3, must not exist
			req.RespondError(err.Error())
			return
		}

		// Put the block in the cache
		err = srv.cacheSet(req.Tc.Score, b)
		if err != nil {
			req.RespondError(err.Error())
			return
		}

		req.RespondRead(b)
		return
	}

	req.RespondRead(b)
}

func (srv *S3Venti) Write(req *vtsrv.Req) {

	s := srv.calcScore(req.Tc.Data)

	// Is the block in the cache?
	b, err := srv.db.Get(nil, s)

	if err != nil {
		req.RespondError(err.Error())
		return
	}

	if b != nil {
		// Block was in the cache, so we can be done.
		req.RespondWrite(s)
		return
	}

	// Block is not in cache. Is it on S3?
	exists, err := srv.bucket.Exists(s.String())

	if err != nil {
		req.RespondError(err.Error())
		return
	}

	if !exists {
		// Block does not exist on S3, so store it there
		err = srv.bucket.Put(s.String(), req.Tc.Data, "binary/octet-stream", s3.BucketOwnerFull, s3.Options{})
		if err != nil {
			req.RespondError(err.Error())
			return
		}
	}

	// Store the block in the cache

	err = srv.cacheSet(s, req.Tc.Data)
	if err != nil {
		req.RespondError(err.Error())
		return
	}

	req.RespondWrite(s)
}

func (srv *S3Venti) cacheSet(key, value []byte) (err error) {
	err = srv.db.BeginTransaction()
	if err != nil {
		return err
	}

	err = srv.db.Set(key, value)
	if err != nil {
		return err
	}

	err = srv.db.Commit()
	if err != nil {
		return err
	}

	return err
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
