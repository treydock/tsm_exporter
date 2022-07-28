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

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treydock/tsm_exporter/config"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	replicationviewTimeout     = kingpin.Flag("collector.replicationview.timeout", "Timeout for collecting replicationview information").Default("5").Int()
	DsmadmcReplicationViewExec = dsmadmcReplicationView
)

type ReplicationViewMetric struct {
	NodeName        string
	FsName          string
	StartTime       float64
	EndTime         float64
	Duration        float64
	ReplicatedBytes float64
	ReplicatedFiles float64
	CompState       string
}

type ReplicationViewCollector struct {
	StartTime                 *prometheus.Desc
	StartTimeIncomplete       *prometheus.Desc
	EndTime                   *prometheus.Desc
	Duration                  *prometheus.Desc
	ReplicatedBytes           *prometheus.Desc
	ReplicatedFiles           *prometheus.Desc
	ReplicatedFilesIncomplete *prometheus.Desc
	target                    *config.Target
	logger                    log.Logger
}

func init() {
	registerCollector("replicationview", true, NewReplicationViewExporter)
}

func NewReplicationViewExporter(target *config.Target, logger log.Logger) Collector {
	labels := []string{"nodename", "fsname"}
	return &ReplicationViewCollector{
		StartTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "start_timestamp_seconds"),
			"Start time of replication", labels, nil),
		StartTimeIncomplete: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "incomplete_start_timestamp_seconds"),
			"Start time of incomplete replication", labels, nil),
		EndTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "end_timestamp_seconds"),
			"End time of replication", labels, nil),
		Duration: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "duration_seconds"),
			"Amount of time taken to complete the most recent replication", labels, nil),
		ReplicatedBytes: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "replicated_bytes"),
			"Amount of data replicated in bytes", labels, nil),
		ReplicatedFiles: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "replicated_files"),
			"Number of files replicated", labels, nil),
		ReplicatedFilesIncomplete: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "incomplete_replicated_files"),
			"Number of files replicated for incomplete", labels, nil),
		target: target,
		logger: logger,
	}
}

func (c *ReplicationViewCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.StartTime
	ch <- c.StartTimeIncomplete
	ch <- c.EndTime
	ch <- c.Duration
	ch <- c.ReplicatedBytes
	ch <- c.ReplicatedFiles
	ch <- c.ReplicatedFilesIncomplete
}

func (c *ReplicationViewCollector) Collect(ch chan<- prometheus.Metric) {
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
		if m.CompState == "INCOMPLETE" {
			ch <- prometheus.MustNewConstMetric(c.StartTimeIncomplete, prometheus.GaugeValue, m.StartTime, m.NodeName, m.FsName)
			ch <- prometheus.MustNewConstMetric(c.ReplicatedFilesIncomplete, prometheus.GaugeValue, m.ReplicatedFiles, m.NodeName, m.FsName)
		} else {
			ch <- prometheus.MustNewConstMetric(c.StartTime, prometheus.GaugeValue, m.StartTime, m.NodeName, m.FsName)
			ch <- prometheus.MustNewConstMetric(c.EndTime, prometheus.GaugeValue, m.EndTime, m.NodeName, m.FsName)
			ch <- prometheus.MustNewConstMetric(c.Duration, prometheus.GaugeValue, m.Duration, m.NodeName, m.FsName)
			ch <- prometheus.MustNewConstMetric(c.ReplicatedBytes, prometheus.GaugeValue, m.ReplicatedBytes, m.NodeName, m.FsName)
			ch <- prometheus.MustNewConstMetric(c.ReplicatedFiles, prometheus.GaugeValue, m.ReplicatedFiles, m.NodeName, m.FsName)
		}
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "replicationview")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "replicationview")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "replicationview")
}

func (c *ReplicationViewCollector) collect() (map[string]ReplicationViewMetric, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*replicationviewTimeout)*time.Second)
	defer cancel()
	out, err := DsmadmcReplicationViewExec(c.target, ctx, c.logger)
	if err != nil {
		return nil, err
	}
	metrics, err := replicationviewParse(out, c.target, c.logger)
	return metrics, err
}

func buildReplicationViewQuery(target *config.Target) string {
	query := "SELECT NODE_NAME, FSNAME, START_TIME, END_TIME, TOTFILES_REPLICATED, TOTBYTES_REPLICATED, COMP_STATE FROM replicationview"
	if target.ReplicationNodeNames != nil {
		query = query + fmt.Sprintf(" WHERE NODE_NAME IN (%s)", buildInFilter(target.ReplicationNodeNames))
	}
	query = query + " ORDER BY END_TIME DESC"
	return query
}

func dsmadmcReplicationView(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	out, err := dsmadmcQuery(target, buildReplicationViewQuery(target), ctx, logger)
	return out, err
}

func replicationviewParse(out string, target *config.Target, logger log.Logger) (map[string]ReplicationViewMetric, error) {
	metrics := make(map[string]ReplicationViewMetric)
	records, err := getRecords(out, logger)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if len(record) != 7 {
			continue
		}
		var metric ReplicationViewMetric
		nodeName := record[0]
		fsName := record[1]
		compState := record[6]
		key := fmt.Sprintf("%s-%s-%s", nodeName, fsName, compState)
		if _, ok := metrics[key]; ok {
			continue
		}
		startTime, err := parseTime(record[2], target)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to parse START_TIME", "value", record[2], "record", strings.Join(record, ","), "err", err)
			return nil, err
		}
		endTime, err := parseTime(record[3], target)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to parse END_TIME", "value", record[3], "record", strings.Join(record, ","), "err", err)
			return nil, err
		}
		replicatedFiles, err := parseFloat(record[4])
		if err != nil {
			level.Error(logger).Log("msg", "Error parsing replicated files", "value", record[4], "record", strings.Join(record, ","), "err", err)
			return nil, err
		}
		replicatedBytes, err := parseFloat(record[5])
		if err != nil {
			level.Error(logger).Log("msg", "Error parsing replicated bytes", "value", record[5], "record", strings.Join(record, ","), "err", err)
			return nil, err
		}
		metric.NodeName = nodeName
		metric.FsName = fsName
		metric.StartTime = float64(startTime.Unix())
		metric.EndTime = float64(endTime.Unix())
		metric.Duration = endTime.Sub(startTime).Seconds()
		metric.ReplicatedFiles = replicatedFiles
		metric.ReplicatedBytes = replicatedBytes
		metric.CompState = compState
		metrics[key] = metric
	}
	return metrics, nil
}
