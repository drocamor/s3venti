// Create a list of all the scores in the bucket and upload that to the bucket

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

	max := 1000
	marker := ""
	chunkCount := max

	var allKeys []string

	for chunkCount >= max {
		chunk, err := bucket.List("", "", marker, max)
		if err != nil {
			fmt.Println("There was an error:", err)
		}

		for _, key := range chunk.Contents {
			allKeys = append(allKeys, key.Key)
		}

		chunkCount = len(chunk.Contents)
		marker = chunk.Contents[chunkCount-1].Key

		fmt.Printf("chunk length was %d. allKeys length is: %d.\n", chunkCount, len(allKeys))
	}

	fmt.Println("Encoding data...")
	gobData := new(bytes.Buffer)

	enc := gob.NewEncoder(gobData)
	enc.Encode(allKeys)

	fmt.Printf("Data size is: %d\n", len(gobData.Bytes()))

	fmt.Println("Storing key list to S3...")
	err = bucket.PutReader("allKeys.gob", gobData, int64(gobData.Len()), "binary/octet-stream", s3.BucketOwnerFull, s3.Options{})
	if err != nil {
		panic(err)
	}

}
