// Copyright 2010 The Govt Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"code.google.com/p/govt/vt"
	"code.google.com/p/govt/vt/vtsrv"
	"crypto/sha1"
	"encoding/gob"
	"flag"
	"fmt"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"hash"
)

type Vts3 struct {
	vtsrv.Srv
	schan  chan hash.Hash
	bucket *s3.Bucket
}

type Block struct {
	Btype uint8
	Score vt.Score
	Data  []byte
}

var addr = flag.String("addr", ":17034", "network address")
var debug = flag.Int("debug", 0, "print debug messages")
var bucketName = flag.String("bucket", "daves-venti", "s3 bucket")

func (srv *Vts3) init() {
	srv.schan = make(chan hash.Hash, 32)

	auth, err := aws.EnvAuth()
	if err != nil {
		panic(err.Error())
	}

	s := s3.New(auth, aws.USEast)
	srv.bucket = s.Bucket(*bucketName)

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

	blockData, err := srv.bucket.Get(blockPath)
	if err != nil {
		fmt.Printf("s3 error:%s\n", err)
		return b
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
		err = srv.bucket.PutReader(blockPath, m, int64(m.Len()), "binary/octet-stream", s3.BucketOwnerFull, s3.Options{})
		if err != nil {
			panic(err)
		}
	}

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
