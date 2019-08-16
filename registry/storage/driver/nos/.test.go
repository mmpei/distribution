package main

import (
	_ "fmt"
	_ "io/ioutil"
	_ "os"
)

func main() {
	nosclient := &NosClient{
		endPoint:  "nos.netease.com",
		accessKey: "a7eefc30be4545b8af96e3f40ee08d35",
		secretKey: "795a49a5b8354305b7ccf0a3974ce571",
	}

	bucket := "docker-test1"
	//object := "object"

	nosclient.PutObject(bucket, "a", []byte("hello world"))
	nosclient.PutObject(bucket, "a/b", []byte("hello world"))
	nosclient.PutObject(bucket, "a/c", []byte("hello world"))

	nosclient.ListObjects(bucket, "a", "", "", 10)

	// initResult, err := nosclient.InitMultiUpload(bucket, object)

	// file, err := os.Open("/Users/nofrish/Desktop/Koala.jpg")
	// if err != nil {
	// 	fmt.Println(err.Error())
	// }
	// defer file.Close()

	// finfo, err := file.Stat()
	// if err != nil {
	// 	fmt.Println(err.Error())
	// }
	// buf := make([]byte, finfo.Size())

	// buf, err = ioutil.ReadAll(file)
	// if err != nil {
	// 	fmt.Println(err.Error())
	// }

	// nosclient.UploadPart(bucket, object, initResult.UploadId, 1, buf)
	// nosclient.UploadPart(bucket, object, initResult.UploadId, 2, buf)

	// uploadParts := UploadParts{}
	// uploadParts.Append(UploadPart{PartNumber: 1, Etag: "2b04df3ecc1d94afddff082d139c6f15"})
	// uploadParts.Append(UploadPart{PartNumber: 2, Etag: "2b04df3ecc1d94afddff082d139c6f15"})

	// listPartsResult, err := nosclient.ListUploadParts(bucket, object, initResult.UploadId, 1000, 0)
	// fmt.Println("==", listPartsResult.Bucket)
	// fmt.Println("==", listPartsResult.Parts[0].Etag)

	// completeResult, err := nosclient.CompleteMultiUpload(bucket, object, initResult.UploadId, uploadParts)

	// fmt.Println(completeResult.Key)
}
