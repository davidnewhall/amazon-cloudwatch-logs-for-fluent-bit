// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//	http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"C"
	"fmt"
	"strconv"
	"time"
	"unsafe"

	"github.com/aws/amazon-cloudwatch-logs-for-fluent-bit/cloudwatch"
	"github.com/aws/amazon-kinesis-firehose-for-fluent-bit/plugins"
	"github.com/fluent/fluent-bit-go/output"

	"github.com/sirupsen/logrus"
)
import (
	"strings"
	"sync"
)

type pluginInstance struct {
	start chan sync.WaitGroup
	done  chan bool
	reply chan int
	*cloudwatch.OutputPlugin
}

var (
	pluginInstances []*pluginInstance
)

func addPluginInstance(ctx unsafe.Pointer) error {
	pluginID := len(pluginInstances)

	config := getConfiguration(ctx, pluginID)
	err := config.Validate()
	if err != nil {
		return err
	}

	instance, err := cloudwatch.NewOutputPlugin(config)
	if err != nil {
		return err
	}

	output.FLBPluginSetContext(ctx, pluginID)
	pi := &pluginInstance{
		OutputPlugin: instance,
		start:        make(chan sync.WaitGroup),
		done:         make(chan bool),
		reply:        make(chan int),
	}

	pluginInstances = append(pluginInstances, pi)

	go pi.processResults()

	return nil
}

func getPluginInstance(ctx unsafe.Pointer) *pluginInstance {
	pluginID := output.FLBPluginGetContext(ctx).(int)
	return pluginInstances[pluginID]
}

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return output.FLBPluginRegister(ctx, "cloudwatch", "AWS CloudWatch Fluent Bit Plugin!")
}

func getConfiguration(ctx unsafe.Pointer, pluginID int) cloudwatch.OutputPluginConfig {
	config := cloudwatch.OutputPluginConfig{}
	config.PluginInstanceID = pluginID

	config.LogGroupName = output.FLBPluginConfigKey(ctx, "log_group_name")
	logrus.Infof("[cloudwatch %d] plugin parameter log_group_name = '%s'", pluginID, config.LogGroupName)

	config.DefaultLogGroupName = output.FLBPluginConfigKey(ctx, "default_log_group_name")
	if config.DefaultLogGroupName == "" {
		config.DefaultLogGroupName = "fluentbit-default"
	}

	logrus.Infof("[cloudwatch %d] plugin parameter default_log_group_name = '%s'", pluginID, config.DefaultLogGroupName)

	config.LogStreamPrefix = output.FLBPluginConfigKey(ctx, "log_stream_prefix")
	logrus.Infof("[cloudwatch %d] plugin parameter log_stream_prefix = '%s'", pluginID, config.LogStreamPrefix)

	config.LogStreamName = output.FLBPluginConfigKey(ctx, "log_stream_name")
	logrus.Infof("[cloudwatch %d] plugin parameter log_stream_name = '%s'", pluginID, config.LogStreamName)

	config.DefaultLogStreamName = output.FLBPluginConfigKey(ctx, "default_log_stream_name")
	if config.DefaultLogStreamName == "" {
		config.DefaultLogStreamName = "/fluentbit-default"
	}

	logrus.Infof("[cloudwatch %d] plugin parameter default_log_stream_name = '%s'", pluginID, config.DefaultLogStreamName)

	config.Region = output.FLBPluginConfigKey(ctx, "region")
	logrus.Infof("[cloudwatch %d] plugin parameter region = '%s'", pluginID, config.Region)

	config.LogKey = output.FLBPluginConfigKey(ctx, "log_key")
	logrus.Infof("[cloudwatch %d] plugin parameter log_key = '%s'", pluginID, config.LogKey)

	config.RoleARN = output.FLBPluginConfigKey(ctx, "role_arn")
	logrus.Infof("[cloudwatch %d] plugin parameter role_arn = '%s'", pluginID, config.RoleARN)

	config.AutoCreateGroup = getBoolParam(ctx, "auto_create_group", false)
	logrus.Infof("[cloudwatch %d] plugin parameter auto_create_group = '%v'", pluginID, config.AutoCreateGroup)

	config.NewLogGroupTags = output.FLBPluginConfigKey(ctx, "new_log_group_tags")
	logrus.Infof("[cloudwatch %d] plugin parameter new_log_group_tags = '%s'", pluginID, config.NewLogGroupTags)

	config.LogRetentionDays = getIntParam(ctx, "log_retention_days", 0)
	logrus.Infof("[cloudwatch %d] plugin parameter log_retention_days = '%d'", pluginID, config.LogRetentionDays)

	config.CWEndpoint = output.FLBPluginConfigKey(ctx, "endpoint")
	logrus.Infof("[cloudwatch %d] plugin parameter endpoint = '%s'", pluginID, config.CWEndpoint)

	config.STSEndpoint = output.FLBPluginConfigKey(ctx, "sts_endpoint")
	logrus.Infof("[cloudwatch %d] plugin parameter sts_endpoint = '%s'", pluginID, config.STSEndpoint)

	config.CredsEndpoint = output.FLBPluginConfigKey(ctx, "credentials_endpoint")
	logrus.Infof("[cloudwatch %d] plugin parameter credentials_endpoint = %s", pluginID, config.CredsEndpoint)

	config.LogFormat = output.FLBPluginConfigKey(ctx, "log_format")
	logrus.Infof("[cloudwatch %d] plugin parameter log_format = '%s'", pluginID, config.LogFormat)

	config.WorkerPoolBuffer = getIntParam(ctx, "worker_pool_buffer", 3000)
	logrus.Infof("[cloudwatch %d] plugin parameter worker_pool_buffer = '%d'", pluginID, config.WorkerPoolBuffer)

	config.WorkerPoolThreads = getIntParam(ctx, "worker_pool_threads", 4)
	logrus.Infof("[cloudwatch %d] plugin parameter worker_pool_threads = '%d'", pluginID, config.WorkerPoolThreads)

	return config
}

func getIntParam(ctx unsafe.Pointer, param string, defaultVal int64) int64 {
	val, _ := strconv.ParseInt(output.FLBPluginConfigKey(ctx, param), 10, 64)
	if val == 0 {
		val = defaultVal
	}

	return val
}

func getBoolParam(ctx unsafe.Pointer, param string, defaultVal bool) bool {
	val := strings.ToLower(output.FLBPluginConfigKey(ctx, param))
	if val == "true" {
		return true
	} else if val == "false" {
		return false
	} else {
		return defaultVal
	}
}

//export FLBPluginInit
func FLBPluginInit(ctx unsafe.Pointer) int {
	plugins.SetupLogger()

	err := addPluginInstance(ctx)
	if err != nil {
		logrus.Error(err)
		return output.FLB_ERROR
	}
	return output.FLB_OK
}

func (cwl *pluginInstance) processResults() {
	var (
		retRes int // set in loop
		retVal int // returned
		wg     sync.WaitGroup
	)

	for {
		select {
		case wg = <-cwl.start: // set the waitgroup.
			retVal = -1 // reset return value.
		case retRes = <-cwl.Results:
			if retRes != output.FLB_OK && retVal != output.FLB_ERROR {
				retVal = retRes
			}

			wg.Done()
		case <-cwl.done:
			cwl.reply <- retVal
		}
	}
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	var (
		count     int
		ret       int
		ts        interface{}
		record    map[interface{}]interface{}
		timestamp time.Time
		wg        sync.WaitGroup

		dec       = output.NewDecoder(data, int(length)) // Create Fluent Bit decoder.
		cwl       = getPluginInstance(ctx)               // Get our plugin instance.
		fluentTag = C.GoString(tag)                      // Extract the tag.
	)

	logrus.Debugf("[cloudwatch %d] Found logs with tag: %s", cwl.PluginInstanceID, fluentTag)

	cwl.start <- wg

	for {
		// Extract Record
		ret, ts, record = output.GetRecord(dec)
		if ret != 0 {
			break
		}

		switch tts := ts.(type) {
		case output.FLBTime:
			timestamp = tts.Time
		case uint64:
			// when ts is of type uint64 it appears to
			// be the amount of seconds since unix epoch.
			timestamp = time.Unix(int64(tts), 0)
		default:
			timestamp = time.Now()
		}

		wg.Add(1)

		cwl.Events <- &cloudwatch.Event{Tag: fluentTag, Record: record, TS: timestamp}
		count++
	}

	logrus.Debugf("[cloudwatch %d] Processing %d events", cwl.PluginInstanceID, count)

	wg.Wait()
	cwl.done <- true

	if ret = <-cwl.reply; ret != output.FLB_OK {
		return ret
	}

	if err := cwl.Flush(); err != nil {
		fmt.Println(err)
		// TODO: Better error handling
		return output.FLB_RETRY
	}

	logrus.Debugf("[cloudwatch %d] Successfully processed %d events", cwl.PluginInstanceID, count)

	// Return options:
	//
	// output.FLB_OK    = data have been processed.
	// output.FLB_ERROR = unrecoverable error, do not try this again.
	// output.FLB_RETRY = retry to flush later.
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {
}
