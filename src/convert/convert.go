package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"

	helper "kzn"

	"github.com/aiden-sobey/parquet-go/writer"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
)

// Environment Variables
var region string = os.Getenv("AWS_REGION")

const CuratedPath = "curated"

type Upload struct {
	localPath string
	bucket    string
	key       string
}

type Row struct {
	Name  string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Age   int32  `parquet:"name=age, type=INT32, convertedtype=INT_32, encoding=PLAIN"`
	Level int32  `parquet:"name=level, type=INT32, convertedtype=INT_32, encoding=PLAIN"`
}

/*
type Row struct {
	day           int32 `parquet:"name=day, type=INT32, convertedtype=INT_32, encoding=PLAIN"`
	aircraft      int32 `parquet:"name=aircraft, type=INT32, convertedtype=INT_32, encoding=PLAIN"`
	helicopter    int32 `parquet:"name=helicopter, type=INT32, convertedtype=INT_32, encoding=PLAIN"`
}
*/

func addLine(w writer.ParquetWriter, schema Row, line []string) {
	row := Row{
		Name:  line[0],
		Age:   helper.ParseInt32(line[1]),
		Level: helper.ParseInt32(line[2]),
	}

	err := w.Write(&row)
	if err != nil {
		helper.Raise(err)
	}
}

var awsSession *session.Session = helper.BuildSession(region)

func main() {
	lambda.Start(handler)
}

func handler(ctx context.Context, s3Event events.S3Event) {
	for _, record := range s3Event.Records {
		s3 := record.S3
		bucket := s3.Bucket.Name
		key := s3.Object.Key

		localPath := downloadS3(bucket, key)
		localPath = csv2parquet(localPath)

		outputKey := fmt.Sprintf(
			"%v%v%v%v.parquet",
			CuratedPath,
			helper.GetZonePath(key),
			helper.GetDatePartition(),
			helper.GetFileName(key),
		)
		log.Printf("Uploading to: %v\n", outputKey)

		uploadS3(&Upload{
			localPath: localPath,
			bucket:    bucket,
			key:       outputKey,
		})

		fmt.Fprint(os.Stdout, localPath)
	}
}

func uploadS3(data *Upload) {
	log.Println("Beginning data upload")
	// Open the file for reading
	file, err := os.Open(data.localPath)
	if err != nil {
		helper.Raise(err)
	}

	uploader := s3manager.NewUploader(awsSession)

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: &data.bucket,
		Key:    &data.key,
		Body:   file,
	})
	if err != nil {
		helper.Raise(err)
	}

	log.Printf("%v uploaded to %v", data.localPath, data.key)
}

func downloadS3(bucket string, key string) string {
	file, err := os.Create("/tmp/file.csv")
	if err != nil {
		helper.Raise(err)
	}

	defer file.Close()

	downloader := s3manager.NewDownloader(awsSession)

	_, err = downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		helper.Raise(err)
	}

	log.Printf("%v downloaded to %v", key, file.Name())
	return file.Name()
}

func csv2parquet(localPath string) string {
	var err error
	outputPath := "/tmp/latest.parquet"

	fw, err := local.NewLocalFileWriter(outputPath)
	if err != nil {
		helper.Raise(err)
	}

	writer, err := writer.NewParquetWriter(fw, new(Row), 2)
	if err != nil {
		helper.Raise(err)
	}

	writer.CompressionType = parquet.CompressionCodec_SNAPPY

	csvFile, _ := os.Open(localPath)
	reader := csv.NewReader(bufio.NewReader(csvFile))
	header := true

	for {
		line, err := reader.Read()

		if err == io.EOF {
			break
		} else if err != nil {
			helper.Raise(err)
		} else if header {
			header = false
			continue
		} else {
			addLine(*writer, Row{}, line)
		}
	}
	log.Println("All rows processed.")

	err = writer.WriteStop()
	if err != nil {
		helper.Raise(err)
	}

	fw.Close()
	log.Printf("File written to %v", localPath)

	return outputPath
}
