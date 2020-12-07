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
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treydock/tsm_exporter/config"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	summaryTimeout     = kingpin.Flag("collector.summary.timeout", "Timeout for collecting summary information").Default("5").Int()
	DsmadmcSummaryExec = dsmadmcSummary
)

type SummaryMetric struct {
	activity string
	entity   string
	schedule string
	endTime  float64
	bytes    float64
}

type SummaryCollector struct {
	endTime *prometheus.Desc
	bytes   *prometheus.Desc
	target  *config.Target
	logger  log.Logger
}

func init() {
	registerCollector("summary", true, NewSummaryExporter)
}

func NewSummaryExporter(target *config.Target, logger log.Logger) Collector {
	labels := []string{"activity", "entity", "schedule"}
	return &SummaryCollector{
		endTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, "summary", "end_timestamp_seconds"),
			"End time of last backup", labels, nil),
		bytes: prometheus.NewDesc(prometheus.BuildFQName(namespace, "summary", "bytes"),
			"Amount of data backed up in last 24 hours", labels, nil),
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
		ch <- prometheus.MustNewConstMetric(c.endTime, prometheus.GaugeValue, m.endTime, labels...)
		ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.GaugeValue, m.bytes, labels...)
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "summary")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "summary")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "summary")
}

func (c *SummaryCollector) collect() (map[string]SummaryMetric, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*summaryTimeout)*time.Second)
	defer cancel()
	out, err := DsmadmcSummaryExec(c.target, ctx, c.logger)
	if err != nil {
		return nil, err
	}
	metrics, err := summaryParse(out, c.target, c.logger)
	return metrics, err
}

func buildSummaryQuery(target *config.Target) string {
	query := "SELECT ACTIVITY,ENTITY,SCHEDULE_NAME,SUM(BYTES),MAX(END_TIME) FROM SUMMARY_EXTENDED"
	if target.SummaryActivities != nil {
		query = query + fmt.Sprintf(" WHERE ACTIVITY IN (%s)", buildInFilter(target.SummaryActivities))
	} else {
		query = query + " WHERE ACTIVITY NOT IN ('TAPE MOUNT','EXPIRATION','PROCESS_START','PROCESS_END') AND ACTIVITY NOT LIKE 'SUR_%'"
	}
	query = query + " GROUP BY ACTIVITY,ENTITY,SCHEDULE_NAME,DATE(END_TIME) ORDER BY DATE(END_TIME) DESC"
	return query
}

func dsmadmcSummary(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	out, err := dsmadmcQuery(target, buildSummaryQuery(target), ctx, logger)
	return out, err
}

func summaryParse(out string, target *config.Target, logger log.Logger) (map[string]SummaryMetric, error) {
	metrics := make(map[string]SummaryMetric)
	records, err := getRecords(out, logger)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if len(record) != 5 {
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
