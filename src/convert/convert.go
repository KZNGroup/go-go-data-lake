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

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
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
	day           int32 `parquet:"name=day, type=INT32, convertedtype=INT_32"`
	aircraft      int32 `parquet:"name=aircraft, type=INT32, convertedtype=INT_32"`
	helicopter    int32 `parquet:"name=helicopter, type=INT32, convertedtype=INT_32"`
	tank          int32 `parquet:"name=tank, type=INT32, convertedtype=INT_32"`
	apc           int32 `parquet:"name=apc, type=INT32, convertedtype=INT_32"`
	artillery     int32 `parquet:"name=artillery, type=INT32, convertedtype=INT_32"`
	mrl           int32 `parquet:"name=mrl, type=INT32, convertedtype=INT_32"`
	military_auto int32 `parquet:"name=military_auto, type=INT32, convertedtype=INT_32"`
	fuel_tank     int32 `parquet:"name=fuel_tank, type=INT32, convertedtype=INT_32"`
	drone         int32 `parquet:"name=drone, type=INT32, convertedtype=INT_32"`
	ship          int32 `parquet:"name=ship, type=INT32, convertedtype=INT_32"`
	anti_aircraft int32 `parquet:"name=anti_aircraft, type=INT32, convertedtype=INT_32"`
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

	writer.RowGroupSize = 128 * 1024 * 1024 //128M
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
		}
		/*
					Get each field in a struct
					var reply interface{} = Point{1, 2}
					t := reflect.TypeOf(reply)
					for i := 0; i < t.NumField(); i++ {
			    		fmt.Printf("%+v\n", t.Field(i))
					}

		*/

		row := Row{
			day:           helper.ParseInt32(line[0]),
			aircraft:      helper.ParseInt32(line[1]),
			helicopter:    helper.ParseInt32(line[2]),
			tank:          helper.ParseInt32(line[3]),
			apc:           helper.ParseInt32(line[4]),
			artillery:     helper.ParseInt32(line[5]),
			mrl:           helper.ParseInt32(line[6]),
			military_auto: helper.ParseInt32(line[7]),
			fuel_tank:     helper.ParseInt32(line[8]),
			drone:         helper.ParseInt32(line[9]),
			ship:          helper.ParseInt32(line[10]),
			anti_aircraft: helper.ParseInt32(line[11]),
		}
		err = writer.Write(row)
		if err != nil {
			helper.Raise(err)
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
