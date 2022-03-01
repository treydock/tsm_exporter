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
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treydock/tsm_exporter/config"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	logTimeout     = kingpin.Flag("collector.log.timeout", "Timeout for collecting log information").Default("10").Int()
	DsmadmcLogExec = dsmadmcLog
	logMap         = map[string]string{
		"TOTAL_SPACE_MB": "Total",
		"USED_SPACE_MB":  "Used",
		"FREE_SPACE_MB":  "Free",
	}
)

type LogMetric struct {
	Total float64
	Used  float64
	Free  float64
}

type LogCollector struct {
	Total  *prometheus.Desc
	Used   *prometheus.Desc
	Free   *prometheus.Desc
	target *config.Target
	logger log.Logger
}

func init() {
	registerCollector("log", true, NewLogExporter)
}

func NewLogExporter(target *config.Target, logger log.Logger) Collector {
	return &LogCollector{
		Total: prometheus.NewDesc(prometheus.BuildFQName(namespace, "active_log", "total_bytes"),
			"Active log total space in bytes", nil, nil),
		Used: prometheus.NewDesc(prometheus.BuildFQName(namespace, "active_log", "used_bytes"),
			"Active log used space in bytes", nil, nil),
		Free: prometheus.NewDesc(prometheus.BuildFQName(namespace, "active_log", "free_bytes"),
			"Active log free space in bytes", nil, nil),
		target: target,
		logger: logger,
	}
}

func (c *LogCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.Total
	ch <- c.Used
	ch <- c.Free
}

func (c *LogCollector) Collect(ch chan<- prometheus.Metric) {
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

	if err == nil {
		ch <- prometheus.MustNewConstMetric(c.Total, prometheus.GaugeValue, metrics.Total)
		ch <- prometheus.MustNewConstMetric(c.Used, prometheus.GaugeValue, metrics.Used)
		ch <- prometheus.MustNewConstMetric(c.Free, prometheus.GaugeValue, metrics.Free)
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "log")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "log")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "log")
}

func (c *LogCollector) collect() (LogMetric, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*logTimeout)*time.Second)
	defer cancel()
	out, err := DsmadmcLogExec(c.target, ctx, c.logger)
	if err != nil {
		return LogMetric{}, err
	}
	metrics, err := logParse(out, c.logger)
	return metrics, err
}

func dsmadmcLog(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	fields := getLogFields()
	query := fmt.Sprintf("SELECT %s FROM log", strings.Join(fields, ","))
	out, err := dsmadmcQuery(target, query, ctx, logger)
	return out, err
}

func logParse(out string, logger log.Logger) (LogMetric, error) {
	var metric LogMetric
	fields := getLogFields()
	records, err := getRecords(out, logger)
	if err != nil {
		return LogMetric{}, err
	}
	for _, record := range records {
		if len(record) != len(fields) {
			continue
		}
		ps := reflect.ValueOf(&metric) // pointer to struct - addressable
		s := ps.Elem()                 //struct
		for i, k := range fields {
			field := logMap[k]
			f := s.FieldByName(field)
			if f.Kind() == reflect.String {
				f.SetString(record[i])
			} else {
				val, err := parseFloat(record[i])
				if err != nil {
					level.Error(logger).Log("msg", "Error parsing value", "key", k, "value", record[i], "record", strings.Join(record, ","), "err", err)
					return LogMetric{}, err
				}
				if strings.HasSuffix(k, "_MB") {
					valBytes := val * 1024.0 * 1024.0
					f.SetFloat(valBytes)
				} else {
					f.SetFloat(val)
				}
			}
		}
	}
	return metric, nil
}

func getLogFields() []string {
	var fields []string
	for k := range logMap {
		fields = append(fields, k)
	}
	sort.Strings(fields)
	return fields
}
