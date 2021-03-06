package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/http2"
)

/**
  This code sample is to read data from a CSV file and ingest data into a Timestream table. Each line of the CSV file is a record to ingest.
  The record schema is fixed, the format is [dimension_name_1, dimension_value_1, dimension_name_2, dimension_value_2, dimension_name_2, dimension_value_2, measure_name, measure_value, measure_data_type, time, time_unit].
  The code will replace the time in the record with a time in the range [current_epoch_in_seconds - number_of_records * 10, current_epoch_in_seconds].
*/
func main() {

	databaseName := flag.String("database_name", "benchmark", "database name string")
	tableName := flag.String("table_name", "goSdk", "table name string")
	testFileName := flag.String("test_file", "../data/sample.csv", "CSV file containing the data to ingest")
	maxGoRoutinesCount := flag.Int("max_go_routines", 25, "Max go routines to ingest data.")
	cpuProfile := flag.String("cpuprofile", "", "write cpu profile to `file`")

	flag.Parse()

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	/**
	* Recommended Timestream write client SDK configuration:
	*  - Set SDK retry count to 10.
	*  - Use SDK DEFAULT_BACKOFF_STRATEGY
	*  - Request timeout of 20 seconds
	 */

	// Setting 20 seconds for timeout
	tr := &http.Transport{
		ResponseHeaderTimeout: 20 * time.Second,
		// Using DefaultTransport values for other parameters: https://golang.org/pkg/net/http/#RoundTripper
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: 30 * time.Second,
			Timeout:   30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	// So client makes HTTP/2 requests
	err := http2.ConfigureTransport(tr)
	if err != nil {
		return
	}

	sess, err := session.NewSession(&aws.Config{Region: aws.String("us-west-2"), MaxRetries: aws.Int(10), HTTPClient: &http.Client{Transport: tr}})
	var writeSvc = timestreamwrite.New(sess)


	// Describe database.
	describeDatabaseInput := &timestreamwrite.DescribeDatabaseInput{
		DatabaseName: aws.String(*databaseName),
	}

	describeDatabaseOutput, err := writeSvc.DescribeDatabase(describeDatabaseInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
		// Create database if database doesn't exist.
		e, ok := err.(*timestreamwrite.ResourceNotFoundException)
		fmt.Println(e)
		if ok {
			fmt.Println("Creating database")
			createDatabaseInput := &timestreamwrite.CreateDatabaseInput{
				DatabaseName: aws.String(*databaseName),
			}

			_, err = writeSvc.CreateDatabase(createDatabaseInput)

			if err != nil {
				fmt.Println("Error:")
				fmt.Println(err)
			}
		}
	} else {
		fmt.Println("Database exists")
		fmt.Println(describeDatabaseOutput)
	}

	// Describe table.
	describeTableInput := &timestreamwrite.DescribeTableInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
	}
	describeTableOutput, err := writeSvc.DescribeTable(describeTableInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
		e, ok := err.(*timestreamwrite.ResourceNotFoundException)
		fmt.Println(e)
		if ok {
			// Create table if table doesn't exist.
			fmt.Println("Creating the table")
			createTableInput := &timestreamwrite.CreateTableInput{
				DatabaseName: aws.String(*databaseName),
				TableName:    aws.String(*tableName),
			}
			_, err = writeSvc.CreateTable(createTableInput)

			if err != nil {
				fmt.Println("Error:")
				fmt.Println(err)
			}
		}
	} else {
		fmt.Println("Table exists")
		fmt.Println(describeTableOutput)
	}

	csvFile, err := os.Open(*testFileName)
	records := make([]*timestreamwrite.Record, 0)
	if err != nil {
		fmt.Println("Couldn't open the csv file", err)
	}

	// Get current time in nano seconds.
	currentTimeInMilliSeconds := time.Now().UnixNano() / int64(time.Millisecond)
	// Counter for number of records.
	counter := int64(0)
	reader := csv.NewReader(csvFile)
	requestSize := 100000
	var requestBatches []*timestreamwrite.WriteRecordsInput
	// Iterate through the records
	for {
		// Read each record from csv
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
		}
		records = append(records, &timestreamwrite.Record{
			Dimensions: []*timestreamwrite.Dimension{
				{
					Name:  aws.String(record[0]),
					Value: aws.String(record[1]),
				},
				{
					Name:  aws.String(record[2]),
					Value: aws.String(record[3]),
				},
				{
					Name:  aws.String(record[4]),
					Value: aws.String(record[5]),
				},
			},
			MeasureName:      aws.String(record[6]),
			MeasureValue:     aws.String(record[7]),
			MeasureValueType: aws.String(record[8]),
			Time:             aws.String(strconv.FormatInt(currentTimeInMilliSeconds-counter*int64(50), 10)),
			TimeUnit:         aws.String("MILLISECONDS"),
		})

		counter++
		// WriteRecordsRequest has 100 records limit per request.
		if counter%100 == 0 {
			writeRecordsInput := &timestreamwrite.WriteRecordsInput{
				DatabaseName: aws.String(*databaseName),
				TableName:    aws.String(*tableName),
				Records:      records,
			}
			requestBatches = append(requestBatches, writeRecordsInput)
			if requestSize == len(requestBatches) {
				break
			}
			records = make([]*timestreamwrite.Record, 0)
		}
	}

	// For the duration of X min, keep ingesting the same records with updated version.
	for end := time.Now().Add(time.Minute * 10); ; {
		Write(requestBatches, *maxGoRoutinesCount, writeSvc)
		if time.Now().After(end) {
			break
		}
		currentTimeInMilliSeconds = time.Now().UnixNano() / int64(time.Millisecond)
		for i := range requestBatches {
			requestBatches[i].CommonAttributes = &timestreamwrite.Record{Version: aws.Int64(time.Now().UnixNano())}
		}
	}

}

func Write(requestBatches []*timestreamwrite.WriteRecordsInput, maxWriteJobs int, writeSvc *timestreamwrite.TimestreamWrite) {
	numberOfWriteRecordsInputs := len(requestBatches)

	if numberOfWriteRecordsInputs < maxWriteJobs {
		maxWriteJobs = numberOfWriteRecordsInputs
	}

	var wg sync.WaitGroup
	writeJobs := make(chan *timestreamwrite.WriteRecordsInput, maxWriteJobs)

	start := time.Now()
	var failed, ingested = 0, 0
	for i := 0; i < maxWriteJobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for writeJob := range writeJobs {
				if err := writeToTimestream(writeJob, writeSvc); err != 0 {
					failed += err
				} else {
					ingested += len(writeJob.Records)
				}
			}
		}()
	}

	for i := range requestBatches {
		writeJobs <- requestBatches[i]
	}
	// Close channel once all jobs are added
	close(writeJobs)

	wg.Wait()
	elapsed := time.Now().Sub(start)

	fmt.Printf("Records ingested: [%d]  rejected [%v] time(ms): [%v]\n", ingested, failed, elapsed.Milliseconds())
}

func writeToTimestream(writeRecordsInput *timestreamwrite.WriteRecordsInput, writeSvc *timestreamwrite.TimestreamWrite) int {
	_, err := writeSvc.WriteRecords(writeRecordsInput)

	if err != nil {
		if _, ok := err.(awserr.Error); ok {
			return len(writeRecordsInput.Records)
		}

	}
	return 0
}

