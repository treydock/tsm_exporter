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

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
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
	duration     float64
}

type EventsCollector struct {
	notCompleted *prometheus.Desc
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
		duration: prometheus.NewDesc(prometheus.BuildFQName(namespace, "schedule", "duration_seconds"),
			"Amount of time taken to complete the most recent completed scheduled event", []string{"schedule"}, nil),
		target: target,
		logger: logger,
	}
}

func (c *EventsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.notCompleted
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
		ch <- prometheus.MustNewConstMetric(c.duration, prometheus.GaugeValue, m.duration, sched)
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
	metrics := eventsParse(completedOut, notCompletedOut, c.logger)
	return metrics, nil
}

func buildEventsCompletedQuery(target *config.Target) string {
	query := "SELECT schedule_name, actual_start, completed FROM events WHERE"
	if target.Schedules != nil {
		query = query + fmt.Sprintf(" schedule_name IN (%s) AND", buildScheduleFilter(target.Schedules))
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
		query = query + fmt.Sprintf(" schedule_name IN (%s) AND", buildScheduleFilter(target.Schedules))
	}
	query = query + fmt.Sprintf(" DATE(scheduled_start) BETWEEN '%s' AND '%s'", yesterday, today)
	return query
}

func dsmadmcEventsNotCompleted(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	out, err := dsmadmcQuery(target, buildEventsNotCompletedQuery(target), ctx, logger)
	return out, err
}

func buildScheduleFilter(schedules []string) string {
	var values []string
	for _, s := range schedules {
		values = append(values, fmt.Sprintf("'%s'", s))
	}
	return strings.Join(values, ",")
}

func eventsParse(completedOut string, notCompletedOut string, logger log.Logger) map[string]EventMetric {
	metrics := make(map[string]EventMetric)
	statusCond := []string{"Completed", "Future", "Started", "In Progress", "Pending"}
	lines := strings.Split(completedOut, "\n")
	for _, line := range lines {
		items := strings.Split(strings.TrimSpace(line), ",")
		if len(items) != 3 {
			continue
		}
		sched := items[0]
		if _, ok := metrics[sched]; ok {
			continue
		}
		actual_start, err := time.Parse(timeFormat, items[1])
		if err != nil {
			level.Error(logger).Log("msg", "Failed to parse actual start time", "time", items[1], "err", err)
			continue
		}
		completed, err := time.Parse(timeFormat, items[2])
		if err != nil {
			level.Error(logger).Log("msg", "Failed to parse completed time", "time", items[2], "err", err)
			continue
		}
		duration := completed.Sub(actual_start).Seconds()
		var metric EventMetric
		metric.name = sched
		metric.duration = duration
		metrics[sched] = metric
	}
	lines = strings.Split(notCompletedOut, "\n")
	for _, line := range lines {
		items := strings.Split(strings.TrimSpace(line), ",")
		if len(items) != 2 {
			continue
		}
		sched := items[0]
		status := items[1]
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
	return metrics
}
