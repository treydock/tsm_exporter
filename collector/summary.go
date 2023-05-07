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
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treydock/tsm_exporter/config"
)

var (
	summaryTimeout     = kingpin.Flag("collector.summary.timeout", "Timeout for collecting summary information").Default("5").Int()
	DsmadmcSummaryExec = dsmadmcSummary
)

type SummaryMetric struct {
	activity  string
	entity    string
	schedule  string
	volume    string
	drive     string
	startTime float64
	endTime   float64
	bytes     float64
}

type SummaryCollector struct {
	startTime          *prometheus.Desc
	endTime            *prometheus.Desc
	bytes              *prometheus.Desc
	tapeMountStartTime *prometheus.Desc
	tapeMountEndTime   *prometheus.Desc
	target             *config.Target
	logger             log.Logger
}

func init() {
	registerCollector("summary", true, NewSummaryExporter)
}

func NewSummaryExporter(target *config.Target, logger log.Logger) Collector {
	labels := []string{"activity", "entity", "schedule"}
	tapeMountLabels := []string{"volume", "drive"}
	return &SummaryCollector{
		startTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, "summary", "start_timestamp_seconds"),
			"Start time of activity", labels, nil),
		endTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, "summary", "end_timestamp_seconds"),
			"End time of activity", labels, nil),
		bytes: prometheus.NewDesc(prometheus.BuildFQName(namespace, "summary", "bytes"),
			"Amount of data for activity, in last 24 hours", labels, nil),
		tapeMountStartTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, "tape_mount", "start_timestamp_seconds"),
			"Start time of activity", tapeMountLabels, nil),
		tapeMountEndTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, "tape_mount", "end_timestamp_seconds"),
			"End time of activity", tapeMountLabels, nil),
		target: target,
		logger: logger,
	}
}

func (c *SummaryCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.endTime
	ch <- c.bytes
}

func (c *SummaryCollector) Collect(ch chan<- prometheus.Metric) {
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

	for _, m := range metrics {
		labels := []string{m.activity, m.entity, m.schedule}
		if m.activity == "TAPE MOUNT" {
			labels = []string{m.volume, m.drive}
			ch <- prometheus.MustNewConstMetric(c.tapeMountStartTime, prometheus.GaugeValue, m.startTime, labels...)
			ch <- prometheus.MustNewConstMetric(c.tapeMountEndTime, prometheus.GaugeValue, m.endTime, labels...)
		} else {
			ch <- prometheus.MustNewConstMetric(c.startTime, prometheus.GaugeValue, m.startTime, labels...)
			ch <- prometheus.MustNewConstMetric(c.endTime, prometheus.GaugeValue, m.endTime, labels...)
			ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.GaugeValue, m.bytes, labels...)
		}
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "summary")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "summary")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "summary")
}

func (c *SummaryCollector) collect() (map[string]SummaryMetric, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*summaryTimeout)*time.Second)
	defer cancel()
	var summaryOut, tapeMountOut string
	var summaryErr, tapeMountErr error
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		summaryOut, summaryErr = DsmadmcSummaryExec(c.target, false, ctx, c.logger)
	}()
	go func() {
		defer wg.Done()
		tapeMountOut, tapeMountErr = DsmadmcSummaryExec(c.target, true, ctx, c.logger)
	}()
	wg.Wait()
	if summaryErr != nil {
		return nil, summaryErr
	}
	if tapeMountErr != nil {
		return nil, tapeMountErr
	}
	metrics, err := summaryParse(summaryOut, tapeMountOut, c.target, c.logger)
	return metrics, err
}

func buildSummaryQuery(target *config.Target) string {
	query := "SELECT ACTIVITY,ENTITY,SCHEDULE_NAME,SUM(BYTES),MIN(START_TIME),MAX(END_TIME) FROM SUMMARY_EXTENDED"
	if target.SummaryActivities != nil {
		query = query + fmt.Sprintf(" WHERE ACTIVITY IN (%s)", buildInFilter(target.SummaryActivities))
	} else {
		query = query + " WHERE ACTIVITY NOT IN ('TAPE MOUNT','EXPIRATION','PROCESS_START','PROCESS_END') AND ACTIVITY NOT LIKE 'SUR_%'"
	}
	query = query + " GROUP BY ACTIVITY,ENTITY,SCHEDULE_NAME,DATE(START_TIME),DATE(END_TIME) ORDER BY DATE(END_TIME) DESC"
	return query
}

func buildTapeMountQuery() string {
	now := timeNow().Format(timeFormat)
	past := timeNow().Add(-time.Hour * 1).Format(timeFormat)
	query := "SELECT ACTIVITY,VOLUME_NAME,DRIVE_NAME,START_TIME,END_TIME FROM SUMMARY_EXTENDED"
	query = query + " WHERE ACTIVITY IN ('TAPE MOUNT')"
	query = query + fmt.Sprintf(" AND END_TIME BETWEEN '%s' AND '%s'", past, now)
	query = query + " ORDER BY END_TIME DESC"
	return query
}

func dsmadmcSummary(target *config.Target, tapeMount bool, ctx context.Context, logger log.Logger) (string, error) {
	var query string
	if tapeMount {
		query = buildTapeMountQuery()
	} else {
		query = buildSummaryQuery(target)
	}
	out, err := dsmadmcQuery(target, query, ctx, logger)
	return out, err
}

func summaryParse(summary string, tapeMount string, target *config.Target, logger log.Logger) (map[string]SummaryMetric, error) {
	metrics := make(map[string]SummaryMetric)
	summaryRecords, err := getRecords(summary, logger)
	if err != nil {
		return nil, err
	}
	tapeMountRecords, err := getRecords(tapeMount, logger)
	if err != nil {
		return nil, err
	}
	for _, record := range summaryRecords {
		if len(record) != 6 {
			continue
		}
		activity := record[0]
		entity := record[1]
		schedule := record[2]
		key := fmt.Sprintf("%s-%s-%s", activity, entity, schedule)
		if _, ok := metrics[key]; ok {
			continue
		}
		var metric SummaryMetric
		metric.activity = activity
		metric.entity = entity
		metric.schedule = schedule
		bytes, err := parseFloat(record[3])
		if err != nil {
			level.Error(logger).Log("msg", "Error parsing summary bytes", "value", record[3], "record", strings.Join(record, ","), "err", err)
			return nil, err
		}
		metric.bytes = bytes
		startTime, err := parseTime(record[4], target)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to parse START_TIME", "value", record[4], "record", strings.Join(record, ","), "err", err)
			return nil, err
		}
		metric.startTime = float64(startTime.Unix())
		endTime, err := parseTime(record[5], target)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to parse END_TIME", "value", record[5], "record", strings.Join(record, ","), "err", err)
			return nil, err
		}
		metric.endTime = float64(endTime.Unix())
		metrics[key] = metric
	}
	for _, record := range tapeMountRecords {
		if len(record) != 5 {
			continue
		}
		activity := record[0]
		volume := record[1]
		drive := strings.Split(record[2], " ")[0]
		key := fmt.Sprintf("%s-%s", drive, volume)
		if _, ok := metrics[key]; ok {
			continue
		}
		var metric SummaryMetric
		metric.activity = activity
		metric.volume = volume
		metric.drive = drive
		startTime, err := parseTime(record[3], target)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to parse START_TIME", "value", record[3], "record", strings.Join(record, ","), "err", err)
			return nil, err
		}
		metric.startTime = float64(startTime.Unix())
		endTime, err := parseTime(record[4], target)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to parse END_TIME", "value", record[4], "record", strings.Join(record, ","), "err", err)
			return nil, err
		}
		metric.endTime = float64(endTime.Unix())
		metrics[key] = metric
	}

	return metrics, nil
}
