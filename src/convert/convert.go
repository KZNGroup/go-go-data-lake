package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func HandleRequest(_context context.Context) (string, error) {
	return fmt.Sprint("Hi to A"), nil
}

func main() {
	lambda.Start(HandleRequest)
}
