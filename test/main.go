package main

import (
	"log"
	"os"
	"runtime/pprof"
	"time"

	"sync"

	"github.com/aws/amazon-cloudwatch-logs-for-fluent-bit/cloudwatch"
)

// TestCycles is how many AddEvent()s to run.
// 30,000,000 is about 1-2 minutes worth on a macbook pro.
const TestCycles = 30000000

var (
	config = cloudwatch.OutputPluginConfig{
		Region:               "us-west-1",
		LogGroupName:         "log-group-name-$(tag)",
		DefaultLogGroupName:  "default-log-group-name",
		LogStreamPrefix:      "",
		LogStreamName:        "log-stream-name-$(tag[0])-$(ident)-$(kubernetes['app'])",
		DefaultLogStreamName: "default-log-stream-name",
		LogKey:               "",
		RoleARN:              "",
		AutoCreateGroup:      false,
		NewLogGroupTags:      "",
		LogRetentionDays:     60,
		CWEndpoint:           "",
		STSEndpoint:          "",
		CredsEndpoint:        "",
		PluginInstanceID:     0,
		LogFormat:            "",
		WorkerPoolBuffer:     2000,
		WorkerPoolThreads:    6,
	}
)

func main() {
	instance, err := cloudwatch.NewOutputPlugin(config)
	if err != nil {
		log.Fatal(err)
	}

	// override all the cloudwatch methods, so they don't do anything.
	instance.Client = &cwMock{}

	f, err := os.Create("cloudwatchlogs.prof")
	if err != nil {
		log.Fatal(err)
	}

	run(f, instance)
}

func run(file *os.File, instance *cloudwatch.OutputPlugin) {
	// logrus.SetLevel(logrus.DebugLevel)
	pprof.StartCPUProfile(file)
	defer pprof.StopCPUProfile()

	start := time.Now()

	channelEvents(start, instance)
	// addEvents(start, instance)
	log.Println("Elapsed:", time.Since(start))
}

// addEvents runs at single routine speed.
func addEvents(ts time.Time, instance *cloudwatch.OutputPlugin) {
	log.Printf("Testing AddEvent with %d events.", TestCycles)

	for i := 0; i < TestCycles; i++ {
		instance.AddEvent(&cloudwatch.Event{TS: ts, Record: map[interface{}]interface{}{
			"msg":   "log line here",
			"ident": "app-name",
			"kubernetes": map[interface{}]interface{}{
				"app": "mykube",
			},
		}, Tag: "input.0"})
	}
}

// channelEvents uses the worker pool.
func channelEvents(ts time.Time, instance *cloudwatch.OutputPlugin) {
	var wg sync.WaitGroup

	log.Printf("Testing Events channel with %d events.", TestCycles)

	// Must listen for results before sending events.
	go func() {
		for i := 0; i < TestCycles; i++ {
			<-instance.Results
		}
		wg.Done()
	}()
	wg.Add(1)

	for i := 0; i < TestCycles; i++ {
		instance.Events <- &cloudwatch.Event{TS: ts, Record: map[interface{}]interface{}{
			"msg":   "log line here",
			"ident": "app-name",
			"kubernetes": map[interface{}]interface{}{
				"app": "mykube",
			},
		}, Tag: "input.0"}
	}

	log.Printf("Waiting for channel responses.")
	wg.Wait()
}
