// TODO: Add license and show some attribution to govt

package main

import (
	"bytes"
	"code.google.com/p/govt/vt"
	"code.google.com/p/govt/vt/vtsrv"
	"crypto/sha1"
	"encoding/gob"
	"flag"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"hash"
)

type Vts3 struct {
	vtsrv.Srv
	schan  chan hash.Hash
	bucket *s3.Bucket
	mc     *memcache.Client
}

type Block struct {
	Btype uint8
	Score vt.Score
	Data  []byte
}

var addr = flag.String("addr", ":17034", "network address")
var debug = flag.Int("debug", 0, "print debug messages")
var bucketName = flag.String("bucket", "daves-venti", "s3 bucket")
var memcacheServer = flag.String("memcached", "127.0.0.1:11211", "memcached server")
var fillCache = flag.Bool("fillcache", false, "download a list of existing scores to fill cache")

func (srv *Vts3) init() {
	srv.schan = make(chan hash.Hash, 32)

	auth, err := aws.EnvAuth()
	if err != nil {
		panic(err.Error())
	}

	s := s3.New(auth, aws.USEast)
	srv.bucket = s.Bucket(*bucketName)

	srv.mc = memcache.New(*memcacheServer)

	if *fillCache == true {
		fmt.Println("Downloading keylist...")
		gobData, err := srv.bucket.Get("allKeys.gob")
		if err != nil {
			panic(err.Error())
		}

		gobBuffer := bytes.NewBuffer(gobData)

		gobDecoder := gob.NewDecoder(gobBuffer)

		var allKeys []string

		err = gobDecoder.Decode(&allKeys)

		if err != nil {
			panic(err)
		}
		fmt.Println("Filling Cache...")
		for _, key := range allKeys {

			srv.mc.Set(&memcache.Item{Key: fmt.Sprintf("exists-%s", key), Value: []byte(nil)})
		}
		fmt.Println("Done.")

	}

}

func (srv *Vts3) calcScore(data []byte) (ret vt.Score) {
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

func (srv *Vts3) getBlock(score vt.Score) *Block {
	var b *Block

	blockPath := fmt.Sprintf("%s", score)

	var blockData []byte

	cachedBlock, err := srv.mc.Get(blockPath)

	if err == nil {
		fmt.Println("Block cached", blockPath)
		blockData = cachedBlock.Value
	} else {
		fmt.Println("Block not cached", blockPath)
		blockData, err = srv.bucket.Get(blockPath)
		if err != nil {
			fmt.Printf("s3 error:%s\n", err)
			return b
		}
		srv.mc.Set(&memcache.Item{Key: blockPath, Value: blockData})
	}

	p := bytes.NewBuffer(blockData)

	dec := gob.NewDecoder(p)

	err = dec.Decode(&b)
	if err != nil {
		fmt.Printf("decode error:%s\n", err)
		return nil
	}

	return b
}

func (srv *Vts3) putBlock(btype uint8, data []byte) *Block {
	var b *Block

	score := srv.calcScore(data)

	b = new(Block)

	// Does the block already exist?
	blockPath := fmt.Sprintf("%s", score)

	existsCacheKey := fmt.Sprintf("exists-%s", score)

	// if there is no error, it's in the cache
	_, cacheErr := srv.mc.Get(existsCacheKey)

	if cacheErr == nil {
		fmt.Println("Cached:", blockPath)
		b.Score = score
		return b
	}

	exists, err := srv.bucket.Exists(blockPath)
	if err != nil {
		panic(err.Error())
	}

	if exists == true {
		fmt.Println("Exists:", blockPath)
		b.Score = score
	} else {
		fmt.Println("Missing:", blockPath)
		b.Score = score
		b.Btype = btype
		b.Data = data
		//b.next = b

		m := new(bytes.Buffer)
		enc := gob.NewEncoder(m)
		enc.Encode(b)

		blockData := m.Bytes()
		err = srv.bucket.Put(blockPath, blockData, "binary/octet-stream", s3.BucketOwnerFull, s3.Options{})
		if err != nil {
			panic(err)
		}
		srv.mc.Set(&memcache.Item{Key: blockPath, Value: blockData})
	}
	srv.mc.Set(&memcache.Item{Key: existsCacheKey, Value: []byte(nil)})
	return b
}

func (srv *Vts3) Hello(req *vtsrv.Req) {
	req.RespondHello("anonymous", 0, 0)
}

func (srv *Vts3) Read(req *vtsrv.Req) {
	b := srv.getBlock(req.Tc.Score)
	if b == nil {
		req.RespondError("not found")
	} else {
		n := int(req.Tc.Count)
		if n > len(b.Data) {
			n = len(b.Data)
		}

		req.RespondRead(b.Data[0:n])
	}
}

func (srv *Vts3) Write(req *vtsrv.Req) {
	b := srv.putBlock(req.Tc.Btype, req.Tc.Data)
	req.RespondWrite(b.Score)
}

func main() {
	flag.Parse()
	srv := new(Vts3)
	srv.init()
	srv.Debuglevel = *debug
	srv.Start(srv)
	srv.StartStatsServer()
	vtsrv.StartListener("tcp", *addr, &srv.Srv)
}
