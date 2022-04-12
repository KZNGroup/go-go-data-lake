package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type Row struct {
	Day           int32 `parquet:"name=day, type=INT32, convertedtype=INT_32, encoding=PLAIN"`
	Aircraft      int32 `parquet:"name=aircraft, type=INT32, convertedtype=INT_32, encoding=PLAIN"`
	Helicopter    int32 `parquet:"name=helicopter, type=INT32, convertedtype=INT_32, encoding=PLAIN"`
	Tank          int32 `parquet:"name=tank, type=INT32, convertedtype=INT_32, encoding=PLAIN"`
	Apc           int32 `parquet:"name=apc, type=INT32, convertedtype=INT_32, encoding=PLAIN"`
	Artillery     int32 `parquet:"name=artillery, type=INT32, convertedtype=INT_32, encoding=PLAIN"`
	Mrl           int32 `parquet:"name=mrl, type=INT32, convertedtype=INT_32, encoding=PLAIN"`
	Military_auto int32 `parquet:"name=military_auto, type=INT32, convertedtype=INT_32, encoding=PLAIN"`
	Fuel_tank     int32 `parquet:"name=fuel_tank, type=INT32, convertedtype=INT_32, encoding=PLAIN"`
	Drone         int32 `parquet:"name=drone, type=INT32, convertedtype=INT_32, encoding=PLAIN"`
	Ship          int32 `parquet:"name=ship, type=INT32, convertedtype=INT_32, encoding=PLAIN"`
	Anti_aircraft int32 `parquet:"name=anti_aircraft, type=INT32, convertedtype=INT_32, encoding=PLAIN"`
}

var region string = os.Getenv("AWS_REGION")
var tableName string = os.Getenv("TABLE_NAME")

var awsSession *session.Session = BuildSession(region)

func main() {
	lambda.Start(handler)
}

func handler(ctx context.Context, s3Event events.S3Event) {
	log.Println("Handler entered")

	for _, record := range s3Event.Records {
		s3 := record.S3
		bucket := s3.Bucket.Name
		key := s3.Object.Key

		dynamo := dynamodb.New(awsSession)

		rows := readParquet(bucket, key)
		writeToDynamo(dynamo, rows)
		log.Println("Handler complete")
	}
}

func readParquet(bucket string, key string) []Row {
	localPath := downloadS3(bucket, key)

	fr, err := local.NewLocalFileReader(localPath)
	if err != nil {
		Raise(err)
	}

	log.Println("Instantiating parquet reader")
	pq, err := reader.NewParquetReader(fr, new(Row), 1)
	if err != nil {
		Raise(err)
	}

	rowCount := int(pq.GetNumRows())
	rows := make([]Row, rowCount)
	log.Printf("Discovered %v rows\n", rowCount)

	if err = pq.Read(&rows); err != nil {
		Raise(err)
	}
	log.Printf("Memory contains %v objects\n", len(rows))

	return rows
}

func writeToDynamo(dynamo *dynamodb.DynamoDB, rows []Row) {
	log.Printf("Writing %v rows to Dynamo table %v", len(rows), tableName)

	for _, row := range rows {
		av, err := dynamodbattribute.MarshalMap(row)
		if err != nil {
			Raise(err)
		}

		input := &dynamodb.PutItemInput{
			Item:      av,
			TableName: aws.String(tableName),
		}

		_, err = dynamo.PutItem(input)
		if err != nil {
			Raise(err)
		}
	}

	log.Println("DynamoDB write complete")
}

func BuildSession(region string) *session.Session {
	sesh, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)
	if err != nil {
		Raise(err)
	}

	log.Println("Generated AWS session")
	return sesh
}

func Raise(err error) {
	fmt.Fprintf(os.Stderr, "%v\n", err.Error())
	os.Exit(1)
}

func downloadS3(bucket string, key string) string {
	file, err := os.Create("/tmp/file.parquet")
	if err != nil {
		Raise(err)
	}

	defer file.Close()

	downloader := s3manager.NewDownloader(awsSession)

	_, err = downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		Raise(err)
	}

	log.Printf("%v downloaded to %v", key, file.Name())
	return file.Name()
}
