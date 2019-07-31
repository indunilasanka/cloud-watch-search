package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"os"
	"strings"
)

func main() {
	file := createFile()

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	cloudWatchClient := cloudwatchlogs.New(sess)

	streamList, logStreamDescribeError := cloudWatchClient.DescribeLogStreams(&cloudwatchlogs.DescribeLogStreamsInput{
		Descending:          aws.Bool(false),
		Limit:               aws.Int64(50),
		LogGroupName:        aws.String("LOG_GROUP"),
		LogStreamNamePrefix: aws.String("STREAM_PREFIX"),
	})
	if logStreamDescribeError != nil {
		fmt.Println("Got error getting stream details:")
		fmt.Println(logStreamDescribeError.Error())
		os.Exit(1)
	}

	for _, streamEvent := range streamList.LogStreams {

		logEventResponse, err := cloudWatchClient.GetLogEvents(&cloudwatchlogs.GetLogEventsInput{
			Limit:         aws.Int64(10000),
			LogGroupName:  aws.String("LOG_GROUP"),
			LogStreamName: streamEvent.LogStreamName,
		})

		if err != nil {
			fmt.Println("Got error getting log events:")
			fmt.Println(err.Error())
			os.Exit(1)
		}

		for _, logEvent := range logEventResponse.Events {

			splitterResult := strings.Split(*logEvent.Message, "\n")

			for i := range splitterResult {
				if strings.Contains(splitterResult[i], "FILTER_CONDITION") {
					fmt.Println(splitterResult[i])
					writeLogs(file, splitterResult[i])
				}
			}
		}
	}

	closeFile(file)
}

func createFile() *os.File {
	f, err := os.Create("abc.log")
	if err != nil {
		fmt.Println(err)
		return f
	}

	return f
}

func writeLogs(file *os.File, msg string) {
	_, err := file.WriteString(msg + "\n")
	if err != nil {
		fmt.Println(err)
		_ = file.Close()
		return
	}
}

func closeFile(file *os.File) {
	err := file.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
}
