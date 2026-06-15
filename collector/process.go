// Copyright 2020 Trey Dockendorf
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"context"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treydock/tsm_exporter/config"
)

var (
	processTimeout     = kingpin.Flag("collector.process.timeout", "Timeout for collecting process information").Default("10").Int()
	DsmadmcProcessExec = dsmadmcProcess
)

type ProcessMetric struct {
	name           string
	filesProcessed float64
	bytesProcessed float64
}

type ProcessCollector struct {
	count          *prometheus.Desc
	processedFiles *prometheus.Desc
	processedBytes *prometheus.Desc
	target         *config.Target
	logger         log.Logger
}

func init() {
	registerCollector("process", true, NewProcessExporter)
}

func NewProcessExporter(target *config.Target, logger log.Logger) Collector {
	return &ProcessCollector{
		count: prometheus.NewDesc(prometheus.BuildFQName(namespace, "process", "count"),
			"Number of processes", []string{"process"}, nil),
		processedFiles: prometheus.NewDesc(prometheus.BuildFQName(namespace, "process", "processed_files"),
			"Total files processed by process", []string{"process"}, nil),
		processedBytes: prometheus.NewDesc(prometheus.BuildFQName(namespace, "process", "processed_bytes"),
			"Total bytes processed by process", []string{"process"}, nil),
		target: target,
		logger: logger,
	}
}

func (c *ProcessCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.count
	ch <- c.processedFiles
	ch <- c.processedBytes
}

func (c *ProcessCollector) Collect(ch chan<- prometheus.Metric) {
	level.Debug(c.logger).Log("msg", "Collecting metrics")
	collectTime := time.Now()
	timeout := 0
	errorMetric := 0
	metrics, err := c.collect()
	if err == context.DeadlineExceeded {
		timeout = 1
	} else if err != nil {
		level.Error(c.logger).Log("msg", err)
		errorMetric = 1
	}

	// Group metrics by process name for aggregation
	processMetrics := make(map[string]*ProcessMetric)
	// Count how many processes we have for each name
	processCounts := make(map[string]int)
	for _, m := range metrics {
		if _, exists := processMetrics[m.name]; !exists {
			processMetrics[m.name] = &ProcessMetric{
				name: m.name,
			}
		}
		// Accumulate files processed and bytes processed
		processMetrics[m.name].filesProcessed += m.filesProcessed
		processMetrics[m.name].bytesProcessed += m.bytesProcessed
		// Increment the count for this process name
		processCounts[m.name]++
	}

	// Emit metrics for each process
	for _, m := range processMetrics {
		ch <- prometheus.MustNewConstMetric(c.count, prometheus.GaugeValue, float64(processCounts[m.name]), m.name)
		ch <- prometheus.MustNewConstMetric(c.processedFiles, prometheus.GaugeValue, m.filesProcessed, m.name)
		ch <- prometheus.MustNewConstMetric(c.processedBytes, prometheus.GaugeValue, m.bytesProcessed, m.name)
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "process")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "process")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "process")
}

func (c *ProcessCollector) collect() ([]ProcessMetric, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*processTimeout)*time.Second)
	defer cancel()
	out, err := DsmadmcProcessExec(c.target, ctx, c.logger)
	if err != nil {
		return nil, err
	}
	metrics, err := processParse(out, c.logger)
	return metrics, err
}

func dsmadmcProcess(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	query := "SELECT process_num,process,files_processed,bytes_processed,bytes_to_process FROM processes"
	out, err := dsmadmcQuery(target, query, ctx, logger)
	return out, err
}

func processParse(out string, logger log.Logger) ([]ProcessMetric, error) {
	var metrics []ProcessMetric
	records, err := getRecords(out, logger)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if len(record) != 5 {
			continue
		}
		var metric ProcessMetric
		metric.name = record[1]
		filesProcessed, err := parseFloat(record[2])
		if err != nil {
			level.Error(logger).Log("msg", "Error parsing files_processed", "value", record[2], "record", strings.Join(record, ","), "err", err)
			return nil, err
		}
		metric.filesProcessed = filesProcessed
		bytesProcessed, err := parseFloat(record[3])
		if err != nil {
			level.Error(logger).Log("msg", "Error parsing bytes_processed", "value", record[3], "record", strings.Join(record, ","), "err", err)
			return nil, err
		}
		metric.bytesProcessed = bytesProcessed
		metrics = append(metrics, metric)
	}
	return metrics, nil
}
