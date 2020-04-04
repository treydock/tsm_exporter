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
	eventsTimeout            = kingpin.Flag("collector.events.timeout", "Timeout for collecting events information").Default("5").Int()
	useEventDurationCache    = kingpin.Flag("collector.events.duration-cache", "Cache event durations").Default("true").Bool()
	DsmadmcEventsExec        = dsmadmcEvents
	eventsCache              = map[string]map[string]EventMetric{}
	eventsCacheMutex         = sync.RWMutex{}
	eventsDurationCache      = map[string]float64{}
	eventsDurationCacheMutex = sync.RWMutex{}
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
	useCache     bool
}

func init() {
	registerCollector("events", true, NewEventsExporter)
}

func NewEventsExporter(target *config.Target, logger log.Logger, useCache bool) Collector {
	return &EventsCollector{
		notCompleted: prometheus.NewDesc(prometheus.BuildFQName(namespace, "schedule", "not_completed"),
			"Number of scheduled events not completed for today", []string{"schedule"}, nil),
		duration: prometheus.NewDesc(prometheus.BuildFQName(namespace, "schedule", "duration_seconds"),
			"Amount of time taken to complete scheduled event for today, in seconds", []string{"schedule"}, nil),
		target:   target,
		logger:   logger,
		useCache: useCache,
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

	for _, m := range metrics {
		ch <- prometheus.MustNewConstMetric(c.notCompleted, prometheus.GaugeValue, m.notCompleted, m.name)
		ch <- prometheus.MustNewConstMetric(c.duration, prometheus.GaugeValue, m.duration, m.name)
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "events")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "events")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "events")
}

func (c *EventsCollector) collect() (map[string]EventMetric, error) {
	var err error
	var out string
	metrics := make(map[string]EventMetric)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*eventsTimeout)*time.Second)
	defer cancel()
	out, err = DsmadmcEventsExec(c.target, ctx, c.logger)
	if err != nil {
		if c.useCache {
			metrics = eventsReadCache(c.target.Name)
		}
		return metrics, err
	}
	metrics, err = eventsParse(out, c.target, *useEventDurationCache, c.logger)
	if err != nil {
		if c.useCache {
			metrics = eventsReadCache(c.target.Name)
		}
		return metrics, err
	}
	if c.useCache {
		eventsWriteCache(c.target.Name, metrics)
	}
	return metrics, nil
}

func dsmadmcEvents(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	query := "SELECT schedule_name,status,actual_start,completed FROM events "
	now := time.Now().Local()
	today := now.Format("2006-01-02")
	yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")
	query = query + fmt.Sprintf(" WHERE DATE(scheduled_start) BETWEEN '%s' AND '%s'", yesterday, today)
	out, err := dsmadmcQuery(target, query, ctx, logger)
	return out, err
}

func eventsParse(out string, target *config.Target, useCache bool, logger log.Logger) (map[string]EventMetric, error) {
	metrics := make(map[string]EventMetric)
	statusCond := []string{"Completed", "Future", "Started", "In Progress", "Pending"}
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		l := strings.TrimSpace(line)
		items := strings.Split(l, ",")
		if len(items) != 4 {
			continue
		}
		sched := items[0]
		status := items[1]
		if target.Schedules != nil && !sliceContains(target.Schedules, sched) {
			continue
		}
		var metric EventMetric
		durationCacheKey := fmt.Sprintf("%s-%s", target.Name, sched)
		if m, ok := metrics[sched]; ok {
			metric = m
		} else {
			metric.name = sched
		}
		if !sliceContains(statusCond, status) {
			metric.notCompleted++
		}
		if status == "Completed" {
			actual_start, err := time.Parse(timeFormat, items[2])
			if err != nil {
				level.Error(logger).Log("msg", fmt.Sprintf("Failed to parse actual_start time '%v': %s", items[2], err.Error()))
				continue
			}
			completed, err := time.Parse(timeFormat, items[3])
			if err != nil {
				level.Error(logger).Log("msg", fmt.Sprintf("Failed to parse completed time '%v': %s", items[3], err.Error()))
				continue
			}
			duration := completed.Sub(actual_start).Seconds()
			metric.duration = duration
			if useCache {
				eventsDurationCacheMutex.Lock()
				eventsDurationCache[durationCacheKey] = metric.duration
				eventsDurationCacheMutex.Unlock()
			}
		} else if metric.duration != 0 {
			// do nothing, use previous value
		} else if useCache {
			eventsDurationCacheMutex.RLock()
			if d, ok := eventsDurationCache[durationCacheKey]; ok {
				metric.duration = d
			}
			eventsDurationCacheMutex.RUnlock()
		}
		metrics[sched] = metric
	}
	return metrics, nil
}

func eventsReadCache(target string) map[string]EventMetric {
	metrics := make(map[string]EventMetric)
	eventsCacheMutex.RLock()
	if cache, ok := eventsCache[target]; ok {
		metrics = cache
	}
	eventsCacheMutex.RUnlock()
	return metrics
}

func eventsWriteCache(target string, metrics map[string]EventMetric) {
	eventsCacheMutex.Lock()
	eventsCache[target] = metrics
	eventsCacheMutex.Unlock()
}
