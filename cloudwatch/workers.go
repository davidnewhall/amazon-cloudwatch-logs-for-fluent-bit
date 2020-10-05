package cloudwatch

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	fluentbit "github.com/fluent/fluent-bit-go/output"
	"github.com/sirupsen/logrus"
)

const millisecond = int64(time.Millisecond)

func (output *OutputPlugin) startWorkers(workers, buffer int64) {
	output.Events = make(chan *Event, buffer)
	output.finishEvents = make(chan *Event, buffer)
	output.Results = make(chan int)

	// This thread handles updates to things that are not thread safe.
	go output.eventWorkerFinish()

	// Worker pool; handles initial unmarshal and log stream/group name settings.
	for i := int64(0); i < workers; i++ {
		go output.eventWorkerStart()
	}
}

// eventWorkerStart is the worker pool. Things that are thread safe go here.
func (output *OutputPlugin) eventWorkerStart() {
	var (
		data []byte
		err  error
	)

	for e := range output.Events {
		// Step 1: convert the Event data to strings, and check for a log key.
		data, err = output.processRecord(e)
		if err != nil {
			logrus.Errorf("[cloudwatch %d] %v\n", output.PluginInstanceID, err)
			// discard this single bad record and let the batch continue
			output.Results <- fluentbit.FLB_OK

			continue
		}

		// Step 2. Make sure the Event data isn't empty.
		if e.string = logString(data); len(e.string) == 0 {
			logrus.Debugf("[cloudwatch %d] Discarding an event from publishing as it is empty\n", output.PluginInstanceID)
			// discard this single empty record and let the batch continue
			output.Results <- fluentbit.FLB_OK

			continue
		}

		// Step 3. Assign a log group and log stream name to the Event.
		output.setGroupStreamNames(e)

		// Funnel our records back into a single thread that updates maps and slices.
		output.finishEvents <- e
	}
}

// eventWorkerFinish runs in a single thread to protect values that are not thread safe.
func (output *OutputPlugin) eventWorkerFinish() {
	var (
		ok     bool
		err    error
		stream *logStream
	)

	for e := range output.finishEvents {
		// Step 4. Create a missing log group for this Event.
		if _, ok = output.groups[e.group]; !ok {
			logrus.Debugf("[cloudwatch %d] Finding log group: %s", output.PluginInstanceID, e.group)

			if err = output.createLogGroup(e); err != nil {
				logrus.Error(err)
				output.Results <- fluentbit.FLB_ERROR

				continue
			}

			output.groups[e.group] = struct{}{}
		}

		// Step 5. Create or retrieve an existing log stream for this Event.
		stream, err = output.getLogStream(e)
		if err != nil {
			logrus.Errorf("[cloudwatch %d] %v\n", output.PluginInstanceID, err)
			// an error means that the log stream was not created; this is retryable
			output.Results <- fluentbit.FLB_RETRY

			continue
		}

		output.Results <- output.saveEvent(e, stream)
	}
}

func (output *OutputPlugin) saveEvent(e *Event, stream *logStream) int {
	// Step 6. Check batch limits and flush buffer if any of these limits will be exeeded by this log Entry.
	if len(stream.logEvents) == maximumLogEventsPerPut || // countLimit
		(stream.currentByteLength+cloudwatchLen(e.string)) >= maximumBytesPerPut || // sizeLimit
		stream.logBatchSpan(e.TS) >= maximumTimeSpanPerPut { // spanLimit
		if err := output.putLogEvents(stream); err != nil {
			logrus.Errorf("[cloudwatch %d] %v\n", output.PluginInstanceID, err)

			// send failures are retryable
			return fluentbit.FLB_RETRY
		}
	}

	// Step 7. Add this event to the running tally.
	stream.logEvents = append(stream.logEvents, &cloudwatchlogs.InputLogEvent{
		Message:   aws.String(e.string),
		Timestamp: aws.Int64(e.TS.UnixNano() / millisecond), // CloudWatch uses milliseconds since epoch
	})

	stream.currentByteLength += cloudwatchLen(e.string)
	if stream.currentBatchStart == nil || stream.currentBatchStart.After(e.TS) {
		stream.currentBatchStart = &e.TS
	}

	if stream.currentBatchEnd == nil || stream.currentBatchEnd.Before(e.TS) {
		stream.currentBatchEnd = &e.TS
	}

	return fluentbit.FLB_OK
}
