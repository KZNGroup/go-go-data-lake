package kzn

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

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

func ParseInt32(input string) int32 {
	day, err := strconv.ParseInt(input, 10, 32)
	if err != nil {
		Raise(err)
	}

	return int32(day)
}

func Raise(err error) {
	fmt.Fprintf(os.Stderr, "%v\n", err.Error())
	os.Exit(1)
}

func GetZonePath(path string) string {
	sections := strings.Split(path, "/")

	if len(sections) == 2 {
		return "/"
	}

	var paths []string
	for x := 1; x < len(sections)-1; x++ {
		paths = append(paths, sections[x])
	}
	return "/" + strings.Join(paths, "/") + "/"
}

func GetFileName(path string) string {
	sections := strings.Split(path, "/")
	file := sections[len(sections)-1]
	return strings.Split(file, ".")[0]
}
