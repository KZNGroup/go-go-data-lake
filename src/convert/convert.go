package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func main() {
	lambda.Start(handler)
}

func handler(ctx context.Context, s3Event events.S3Event) {
	for _, record := range s3Event.Records {
		s3 := record.S3
		// TODO: use channels to make concurrent
		readS3(s3.Bucket.Name, s3.Object.Key)
	}
}

func readS3(bucket string, key string) string {
	//the only writable directory in the lambda is /tmp
	fmt.Fprintf(os.Stdout, "Processing %v/%v\n", bucket, key)
	file, err := os.Create("/tmp/" + key)
	if err != nil {
		raise(err)
	}

	defer file.Close()

	//replace with your bucket region
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("ap-southeast-2")},
	)

	downloader := s3manager.NewDownloader(sess)

	_, err = downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		raise(err)
	}

	dat, err := ioutil.ReadFile(file.Name())

	if err != nil {
		raise(err)
	}

	return string(dat)

}

func raise(err error) {
	//fmt.Fprintf(os.Stdout, "%v\n", msg)
	fmt.Fprintf(os.Stderr, "%v\n", err.Error())
	os.Exit(1)
}
