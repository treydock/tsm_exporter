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
	"fmt"
	"strconv"
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
	replicationviewTimeout                  = kingpin.Flag("collector.replicationview.timeout", "Timeout for collecting replicationview information").Default("5").Int()
	DsmadmcReplicationViewsCompletedExec    = dsmadmcReplicationViewsCompleted
	DsmadmcReplicationViewsNotCompletedExec = dsmadmcReplicationViewsNotCompleted
)

type ReplicationViewMetric struct {
	NodeName        string
	FsName          string
	StartTime       float64
	EndTime         float64
	NotCompleted    float64
	Duration        float64
	ReplicatedBytes float64
	ReplicatedFiles float64
}

type ReplicationViewsCollector struct {
	NotCompleted    *prometheus.Desc
	StartTime       *prometheus.Desc
	EndTime         *prometheus.Desc
	Duration        *prometheus.Desc
	ReplicatedBytes *prometheus.Desc
	ReplicatedFiles *prometheus.Desc
	target          *config.Target
	logger          log.Logger
}

func init() {
	registerCollector("replicationview", true, NewReplicationViewsExporter)
}

func NewReplicationViewsExporter(target *config.Target, logger log.Logger) Collector {
	return &ReplicationViewsCollector{
		NotCompleted: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "not_completed"),
			"Number of replications not completed for today", []string{"nodename", "fsname"}, nil),
		StartTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "start_timestamp_seconds"),
			"Start time of replication", []string{"nodename", "fsname"}, nil),
		EndTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "end_timestamp_seconds"),
			"End time of replication", []string{"nodename", "fsname"}, nil),
		Duration: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "duration_seconds"),
			"Amount of time taken to complete the most recent replication", []string{"nodename", "fsname"}, nil),
		ReplicatedBytes: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "replicated_bytes"),
			"Amount of data replicated in bytes", []string{"nodename", "fsname"}, nil),
		ReplicatedFiles: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "replicated_files"),
			"Number of files replicated", []string{"nodename", "fsname"}, nil),
		target: target,
		logger: logger,
	}
}

func (c *ReplicationViewsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.NotCompleted
	ch <- c.StartTime
	ch <- c.EndTime
	ch <- c.Duration
	ch <- c.ReplicatedBytes
	ch <- c.ReplicatedFiles
}

func (c *ReplicationViewsCollector) Collect(ch chan<- prometheus.Metric) {
	level.Debug(c.logger).Log("msg", "Collecting metrics")
	collectTime := time.Now()
	errorMetric := 0
	metrics, err := c.collect()
	if err != nil {
		level.Error(c.logger).Log("msg", err)
		errorMetric = 1
	}

	for _, m := range metrics {
		ch <- prometheus.MustNewConstMetric(c.NotCompleted, prometheus.GaugeValue, m.NotCompleted, m.NodeName, m.FsName)
		ch <- prometheus.MustNewConstMetric(c.StartTime, prometheus.GaugeValue, m.StartTime, m.NodeName, m.FsName)
		ch <- prometheus.MustNewConstMetric(c.EndTime, prometheus.GaugeValue, m.EndTime, m.NodeName, m.FsName)
		ch <- prometheus.MustNewConstMetric(c.Duration, prometheus.GaugeValue, m.Duration, m.NodeName, m.FsName)
		ch <- prometheus.MustNewConstMetric(c.ReplicatedBytes, prometheus.GaugeValue, m.ReplicatedBytes, m.NodeName, m.FsName)
		ch <- prometheus.MustNewConstMetric(c.ReplicatedFiles, prometheus.GaugeValue, m.ReplicatedFiles, m.NodeName, m.FsName)
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "replicationview")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "replicationview")
}

func (c *ReplicationViewsCollector) collect() (map[string]ReplicationViewMetric, error) {
	var completedOut, notCompletedOut string
	var completedErr, notCompletedErr error
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		completedOut, completedErr = DsmadmcReplicationViewsCompletedExec(c.target, c.logger)
	}()
	go func() {
		defer wg.Done()
		notCompletedOut, notCompletedErr = DsmadmcReplicationViewsNotCompletedExec(c.target, c.logger)
	}()
	wg.Wait()
	if completedErr != nil {
		return nil, completedErr
	}
	if notCompletedErr != nil {
		return nil, notCompletedErr
	}
	metrics := replicationviewParse(completedOut, notCompletedOut, c.logger)
	return metrics, nil
}

func buildReplicationViewCompletedQuery(target *config.Target) string {
	query := "SELECT NODE_NAME, FSNAME, START_TIME, END_TIME, TOTFILES_REPLICATED, TOTBYTES_REPLICATED FROM replicationview WHERE"
	if target.ReplicationNodeNames != nil {
		query = query + fmt.Sprintf(" NODE_NAME IN (%s) AND", buildInFilter(target.ReplicationNodeNames))
	}
	query = query + " COMP_STATE = 'COMPLETE' ORDER BY END_TIME DESC"
	return query
}

func dsmadmcReplicationViewsCompleted(target *config.Target, logger log.Logger) (string, error) {
	out, err := dsmadmcQuery(target, buildReplicationViewCompletedQuery(target), *replicationviewTimeout, logger)
	return out, err
}

func buildReplicationViewNotCompletedQuery(target *config.Target) string {
	query := "SELECT NODE_NAME, FSNAME FROM replicationview WHERE"
	if target.ReplicationNodeNames != nil {
		query = query + fmt.Sprintf(" NODE_NAME IN (%s) AND", buildInFilter(target.ReplicationNodeNames))
	}
	now := timeNow().Local()
	today := now.Format("2006-01-02")
	yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")
	query = query + fmt.Sprintf(" COMP_STATE <> 'COMPLETE' AND DATE(START_TIME) BETWEEN '%s' AND '%s'", yesterday, today)
	return query
}

func dsmadmcReplicationViewsNotCompleted(target *config.Target, logger log.Logger) (string, error) {
	out, err := dsmadmcQuery(target, buildReplicationViewNotCompletedQuery(target), *replicationviewTimeout, logger)
	return out, err
}

func replicationviewParse(completedOut string, notCompletedOut string, logger log.Logger) map[string]ReplicationViewMetric {
	metrics := make(map[string]ReplicationViewMetric)
	lines := strings.Split(completedOut, "\n")
	for _, line := range lines {
		values := strings.Split(strings.TrimSpace(line), ",")
		if len(values) != 6 {
			continue
		}
		var metric ReplicationViewMetric
		nodeName := values[0]
		fsName := values[1]
		key := fmt.Sprintf("%s-%s", nodeName, fsName)
		if _, ok := metrics[key]; ok {
			continue
		}
		startTime, err := time.Parse(timeFormat, values[2])
		if err != nil {
			level.Error(logger).Log("msg", "Failed to parse START_TIME", "value", values[2], "err", err)
			continue
		}
		endTime, err := time.Parse(timeFormat, values[3])
		if err != nil {
			level.Error(logger).Log("msg", "Failed to parse END_TIME", "value", values[3], "err", err)
			continue
		}
		replicatedFiles, err := strconv.ParseFloat(values[4], 64)
		if err != nil {
			level.Error(logger).Log("msg", "Error parsing replicated files", "value", values[4], "err", err)
			continue
		}
		replicatedBytes, err := strconv.ParseFloat(values[5], 64)
		if err != nil {
			level.Error(logger).Log("msg", "Error parsing replicated bytes", "value", values[5], "err", err)
			continue
		}
		metric.NodeName = nodeName
		metric.FsName = fsName
		metric.StartTime = float64(startTime.Unix())
		metric.EndTime = float64(endTime.Unix())
		metric.Duration = endTime.Sub(startTime).Seconds()
		metric.ReplicatedFiles = replicatedFiles
		metric.ReplicatedBytes = replicatedBytes
		metrics[key] = metric
	}
	lines = strings.Split(notCompletedOut, "\n")
	for _, line := range lines {
		values := strings.Split(strings.TrimSpace(line), ",")
		if len(values) != 2 {
			continue
		}
		var metric ReplicationViewMetric
		nodeName := values[0]
		fsName := values[1]
		key := fmt.Sprintf("%s-%s", nodeName, fsName)
		if m, ok := metrics[key]; ok {
			metric = m
		} else {
			metric.NodeName = nodeName
			metric.FsName = fsName
		}
		metric.NotCompleted++
		metrics[key] = metric
	}
	return metrics
}
