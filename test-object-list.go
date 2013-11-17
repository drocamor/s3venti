//  tests reading the object list

package main

import (
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
)

var bucketName = flag.String("bucket", "daves-venti", "s3 bucket")

func main() {
	flag.Parse()

	auth, err := aws.EnvAuth()
	if err != nil {
		panic(err.Error())
	}

	s := s3.New(auth, aws.USEast)
	bucket := s.Bucket(*bucketName)

	gobData, err := bucket.Get("allKeys.gob")
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

	fmt.Printf("Got %d items. First 10 items:\n", len(allKeys))

	for x := 0; x < 10; x++ {
		fmt.Println(allKeys[x])
	}
}
