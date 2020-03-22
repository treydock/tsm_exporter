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
	eventsTimeout     = kingpin.Flag("collector.events.timeout", "Timeout for collecting events information").Default("5").Int()
	DsmadmcEventsExec = dsmadmcEvents
	eventsCache       = map[string][]EventMetric{}
	eventsCacheMutex  = sync.RWMutex{}
)

type EventMetric struct {
	name       string
	notstarted float64
}

type EventsCollector struct {
	notstarted *prometheus.Desc
	target     *config.Target
	logger     log.Logger
	useCache   bool
}

func init() {
	registerCollector("events", true, NewEventsExporter)
}

func NewEventsExporter(target *config.Target, logger log.Logger, useCache bool) Collector {
	return &EventsCollector{
		notstarted: prometheus.NewDesc(prometheus.BuildFQName(namespace, "schedule", "notstarted"),
			"Number of schedules not started for today", []string{"schedule"}, nil),
		target:   target,
		logger:   logger,
		useCache: useCache,
	}
}

func (c *EventsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.notstarted
}

func (c *EventsCollector) Collect(ch chan<- prometheus.Metric) {
	level.Debug(c.logger).Log("msg", "Collecting events metrics")
	collectTime := time.Now()
	timeout := 0
	errorMetric := 0
	metrics, err := c.collect()
	if err == context.DeadlineExceeded {
		level.Error(c.logger).Log("msg", "Timeout executing dsmadmc")
		timeout = 1
	} else if err != nil {
		level.Error(c.logger).Log("msg", err)
		errorMetric = 1
	}

	for _, m := range metrics {
		ch <- prometheus.MustNewConstMetric(c.notstarted, prometheus.GaugeValue, m.notstarted, m.name)
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "events")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "events")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "events")
}

func (c *EventsCollector) collect() ([]EventMetric, error) {
	var err error
	var out string
	var metrics []EventMetric
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*eventsTimeout)*time.Second)
	defer cancel()
	out, err = DsmadmcEventsExec(c.target, ctx, c.logger)
	if ctx.Err() == context.DeadlineExceeded {
		if c.useCache {
			metrics = eventsReadCache(c.target.Name)
		}
		return metrics, ctx.Err()
	}
	if err != nil {
		if c.useCache {
			metrics = eventsReadCache(c.target.Name)
		}
		return metrics, err
	}
	metrics, err = eventsParse(out, c.target.Schedules, c.logger)
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
	//statusCond := []string{"status<>'Completed'", "status<>'Future'", "status<>'Started'", "status<>'In Progress'", "status<>'Pending'"}
	//statusCond := []string{"status<>'Completed'"}
	query := "SELECT schedule_name,status FROM events "
	//statusQuery := strings.Join(statusCond, " AND ")
	//query = query + statusQuery
	now := time.Now().Local()
	today := now.Format("2006-01-02")
	yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")
	query = query + fmt.Sprintf(" WHERE DATE(scheduled_start) BETWEEN '%s' AND '%s'", yesterday, today)
	/*if target.Schedules != nil {
		var scheduleNames []string
		for _, sched := range target.Schedules {
			s := fmt.Sprintf("schedule_name='%s'", sched)
			scheduleNames = append(scheduleNames, s)
		}
		scheduleQuery := strings.Join(scheduleNames, " OR ")
		query = query + fmt.Sprintf(" AND (%s)", scheduleQuery)
	}*/
	out, err := dsmadmcQuery(target, query, ctx, logger)
	return out, err
}

func eventsParse(out string, schedules []string, logger log.Logger) ([]EventMetric, error) {
	var metrics []EventMetric
	statusCond := []string{"Completed", "Future", "Started", "In Progress", "Pending"}
	scheds := make(map[string]int)
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		l := strings.TrimSpace(line)
		items := strings.Split(l, ",")
		if len(items) != 2 {
			continue
		}
		sched := items[0]
		status := items[1]
		if schedules != nil && !sliceContains(schedules, sched) {
			continue
		}
		if _, ok := scheds[sched]; !ok {
			scheds[sched] = 0
		}
		if !sliceContains(statusCond, status) {
			scheds[sched]++
		}
	}
	for sched, count := range scheds {
		metric := EventMetric{name: sched, notstarted: float64(count)}
		metrics = append(metrics, metric)
	}
	return metrics, nil
}

func eventsReadCache(target string) []EventMetric {
	var metrics []EventMetric
	eventsCacheMutex.RLock()
	if cache, ok := eventsCache[target]; ok {
		metrics = cache
	}
	eventsCacheMutex.RUnlock()
	return metrics
}

func eventsWriteCache(target string, metrics []EventMetric) {
	eventsCacheMutex.Lock()
	eventsCache[target] = metrics
	eventsCacheMutex.Unlock()
}
