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

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treydock/tsm_exporter/config"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	eventsTimeout                 = kingpin.Flag("collector.events.timeout", "Timeout for collecting events information").Default("10").Int()
	DsmadmcEventsCompletedExec    = dsmadmcEventsCompleted
	DsmadmcEventsNotCompletedExec = dsmadmcEventsNotCompleted
)

type EventMetric struct {
	name         string
	notCompleted float64
	start        float64
	completed    float64
	duration     float64
}

type EventsCollector struct {
	notCompleted *prometheus.Desc
	start        *prometheus.Desc
	completed    *prometheus.Desc
	duration     *prometheus.Desc
	target       *config.Target
	logger       log.Logger
}

func init() {
	registerCollector("events", true, NewEventsExporter)
}

func NewEventsExporter(target *config.Target, logger log.Logger) Collector {
	return &EventsCollector{
		notCompleted: prometheus.NewDesc(prometheus.BuildFQName(namespace, "schedule", "not_completed"),
			"Number of scheduled events not completed for today", []string{"schedule"}, nil),
		start: prometheus.NewDesc(prometheus.BuildFQName(namespace, "schedule", "start_timestamp_seconds"),
			"Start time of the most recent completed scheduled event", []string{"schedule"}, nil),
		completed: prometheus.NewDesc(prometheus.BuildFQName(namespace, "schedule", "completed_timestamp_seconds"),
			"Completed time of the most recent completed scheduled event", []string{"schedule"}, nil),
		duration: prometheus.NewDesc(prometheus.BuildFQName(namespace, "schedule", "duration_seconds"),
			"Amount of time taken to complete the most recent completed scheduled event", []string{"schedule"}, nil),
		target: target,
		logger: logger,
	}
}

func (c *EventsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.notCompleted
	ch <- c.start
	ch <- c.completed
	ch <- c.duration
}

func (c *EventsCollector) Collect(ch chan<- prometheus.Metric) {
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

	for sched, m := range metrics {
		ch <- prometheus.MustNewConstMetric(c.notCompleted, prometheus.GaugeValue, m.notCompleted, sched)
		if m.completed != 0 {
			ch <- prometheus.MustNewConstMetric(c.start, prometheus.GaugeValue, m.start, sched)
			ch <- prometheus.MustNewConstMetric(c.completed, prometheus.GaugeValue, m.completed, sched)
			ch <- prometheus.MustNewConstMetric(c.duration, prometheus.GaugeValue, m.duration, sched)
		}
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "events")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "events")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "events")
}

func (c *EventsCollector) collect() (map[string]EventMetric, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*eventsTimeout)*time.Second)
	defer cancel()
	var completedOut, notCompletedOut string
	var completedErr, notCompletedErr error
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		completedOut, completedErr = DsmadmcEventsCompletedExec(c.target, ctx, c.logger)
	}()
	go func() {
		defer wg.Done()
		notCompletedOut, notCompletedErr = DsmadmcEventsNotCompletedExec(c.target, ctx, c.logger)
	}()
	wg.Wait()
	if completedErr != nil {
		return nil, completedErr
	}
	if notCompletedErr != nil {
		return nil, notCompletedErr
	}
	metrics, err := eventsParse(completedOut, notCompletedOut, c.target, c.logger)
	return metrics, err
}

func buildEventsCompletedQuery(target *config.Target) string {
	query := "SELECT schedule_name, actual_start, completed FROM events WHERE"
	if target.Schedules != nil {
		query = query + fmt.Sprintf(" schedule_name IN (%s) AND", buildInFilter(target.Schedules))
	}
	query = query + " status = 'Completed' ORDER BY completed DESC"
	return query
}

func dsmadmcEventsCompleted(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	out, err := dsmadmcQuery(target, buildEventsCompletedQuery(target), ctx, logger)
	return out, err
}

func buildEventsNotCompletedQuery(target *config.Target) string {
	query := "SELECT schedule_name,status FROM events WHERE"
	now := timeNow().Local()
	today := now.Format("2006-01-02")
	yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")
	if target.Schedules != nil {
		query = query + fmt.Sprintf(" schedule_name IN (%s) AND", buildInFilter(target.Schedules))
	}
	query = query + fmt.Sprintf(" DATE(scheduled_start) BETWEEN '%s' AND '%s'", yesterday, today)
	return query
}

func dsmadmcEventsNotCompleted(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	out, err := dsmadmcQuery(target, buildEventsNotCompletedQuery(target), ctx, logger)
	return out, err
}

func eventsParse(completedOut string, notCompletedOut string, target *config.Target, logger log.Logger) (map[string]EventMetric, error) {
	metrics := make(map[string]EventMetric)
	statusCond := []string{"Completed", "Future", "Started", "In Progress", "Pending"}
	records, err := getRecords(completedOut, logger)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if len(record) != 3 {
			continue
		}
		sched := record[0]
		if _, ok := metrics[sched]; ok {
			continue
		}
		start, err := parseTime(record[1], target)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to parse actual start time", "time", record[1], "record", strings.Join(record, ","), "err", err)
			return nil, err
		}
		completed, err := parseTime(record[2], target)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to parse completed time", "time", record[2], "record", strings.Join(record, ","), "err", err)
			return nil, err
		}
		duration := completed.Sub(start).Seconds()
		var metric EventMetric
		metric.name = sched
		metric.start = float64(start.Unix())
		metric.completed = float64(completed.Unix())
		metric.duration = duration
		metrics[sched] = metric
	}
	records, err = getRecords(notCompletedOut, logger)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if len(record) != 2 {
			continue
		}
		sched := record[0]
		status := record[1]
		var metric EventMetric
		if m, ok := metrics[sched]; ok {
			metric = m
		} else {
			metric.name = sched
		}
		if !sliceContains(statusCond, status) {
			metric.notCompleted++
		}
		metrics[sched] = metric
	}
	return metrics, nil
}
