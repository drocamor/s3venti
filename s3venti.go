// TODO: Add license and show some attribution to govt

package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"flag"
	"fmt"
	"hash"
	"log"
	"os"
	"time"

	"code.google.com/p/govt/vt"
	"code.google.com/p/govt/vt/vtsrv"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/twinj/uuid"
)

type Vts3 struct {
	vtsrv.Srv
	schan        chan hash.Hash
	bucket       *s3.Bucket
	debug        int
	putter       chan *Block
	currentChunk Chunk
}

type Block struct {
	Btype uint8
	Score vt.Score
	Data  []byte
}

type Chunk struct {
	Blocks map[string]*Block
	Id     string
}

var addr = flag.String("addr", ":17034", "network address")
var debug = flag.Int("debug", 0, "print debug messages")
var bucketName = flag.String("bucket", "daves-venti", "s3 bucket")

func (c *Chunk) init() {
	// Create a DynamoDB record saying the chunk is new and not uploaded.
	c.Blocks = make(map[string]*Block)
	c.Id = uuid.NewV4().String()
}

// stores stores a chunk to disk
func (c *Chunk) store() {

	if len(c.Blocks) == 0 {
		return
	}

	data := new(bytes.Buffer)
	enc := gob.NewEncoder(data)
	enc.Encode(c)
	filename := fmt.Sprintf("/Users/drocamor/.venti/chunks/%s", c.Id)
	fo, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer fo.Close()

	data.WriteTo(fo)
}

// createBlockPutter makes a goroutine that recieves blocks and stores them in chunks
func (srv *Vts3) createBlockPutter() {
	srv.putter = make(chan *Block)
	go func() {
		for {
			select {
			case b := <-srv.putter:
				score := fmt.Sprintf("%s", b.Score)
				srv.log("putter: storing", score, "in chunk", srv.currentChunk.Id)
				srv.currentChunk.Blocks[score] = b
				// If there are more than 1000 blocks in a chunk, upload and init it
			case <-time.After(10 * time.Second):
				srv.log("Should rotate chunk now")

				// store chunk in a file
				srv.currentChunk.store()
				// make file be uploaded
				// init the currentChunk
				srv.currentChunk.init()
			}
		}
	}()

}

func (srv *Vts3) init() {
	uuid.SwitchFormat(uuid.Clean, false)
	srv.schan = make(chan hash.Hash, 32)

	auth, err := aws.EnvAuth()
	if err != nil {
		panic(err.Error())
	}

	s := s3.New(auth, aws.USEast)
	srv.bucket = s.Bucket(*bucketName)
	srv.currentChunk.init()
	srv.createBlockPutter()

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

	// Check the current chunk
	if b, ok := srv.currentChunk.Blocks[blockPath]; ok {
		return b
	}

	// Check the block->chunk mapping
	// Get the block
	return b
}

func (srv *Vts3) putBlock(btype uint8, data []byte) *Block {
	var b *Block

	score := srv.calcScore(data)

	b = new(Block)
	b.Score = score

	// Does the block already exist?
	blockPath := fmt.Sprintf("%s", score)

	// Check the current chunk
	if _, ok := srv.currentChunk.Blocks[blockPath]; ok {
		srv.log("putBlock: block exists -", blockPath)

		return b
	}

	// Check the block->chunk dynamodb table

	// Can't find it, put the block

	srv.log("putBlock: block missing -", blockPath)
	b.Btype = btype
	b.Data = data

	srv.putter <- b

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

func (srv *Vts3) log(m ...interface{}) {
	if srv.debug > 0 {
		log.Println(m)
	}
}

func main() {
	flag.Parse()
	srv := new(Vts3)
	srv.init()
	srv.debug = *debug
	srv.Start(srv)
	srv.StartStatsServer()
	vtsrv.StartListener("tcp", *addr, &srv.Srv)
}
