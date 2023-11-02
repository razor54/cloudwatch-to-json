package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/one-network/cloudwatch-to-json/logfile"
)

// Process data into a single file
var (
	filePath = "output.txt"
	file     *os.File
	mu       sync.Mutex
	writeCh  chan string
	wg       sync.WaitGroup
)

var pathFlag = flag.String("path", "", "The path to gzipped CloudWatch data (default format of data exported to S3).")

func main() {
	flag.Parse()

	if *pathFlag == "" {
		fmt.Println("Missing -path flag")
		os.Exit(-1)
	}

	cwr := logfile.NewCloudWatchReader(*pathFlag)

	lec := make(chan logfile.Entry, 1024*1024)
	workCompleted := make(chan interface{})

	// File handler

	// Open the file for writing
	var err error
	file, err = os.Create(filePath)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	writeCh = make(chan string, 100) // Buffered channel for asynchronous writes

	// Start a separate goroutine for file writes
	go writeToFile()

	// add 1 element to wait group? a bit ... :(
	wg.Add(1)

	go func() {
		defer wg.Done()

		var read int
		for {
			le, ok := <-lec
			if !ok {
				workCompleted <- true
				return
			}
			read++

			if isJSON(le.Message) && strings.Contains(le.Message, "maricopa") && strings.Contains(le.Message, "ERROR") {
				// maybe we should just write to a file...
				fmt.Println(le.Message)

				// Enqueue the processed data for asynchronous write
				writeCh <- le.Message
			}
		}
	}()

	_, err = cwr.Read(lec)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(-1)
	}
	close(lec)
	<-workCompleted

	wg.Wait()
	close(writeCh) // Close the channel to signal the writer goroutine to exit

}

func isJSON(str string) bool {
	var js json.RawMessage
	return json.Unmarshal([]byte(str), &js) == nil
}

// File processing of data

func writeToFile() {
	for processedData := range writeCh {
		mu.Lock() // Lock the mutex for file write
		writeDataToFile(processedData)
		mu.Unlock() // Unlock the mutex
	}
}

func writeDataToFile(processedData string) {
	// Write the processed data to the file
	_, err := file.WriteString(processedData + "\n") // Do I need the \n ?
	if err != nil {
		fmt.Println("Error writing processed data to file:", err)
	}
}
