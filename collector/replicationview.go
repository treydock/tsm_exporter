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
	replicationviewTimeout          = kingpin.Flag("collector.replicationview.timeout", "Timeout for collecting replicationview information").Default("5").Int()
	useReplicationViewMetricCache   = kingpin.Flag("collector.replicationview.metric-cache", "Cache replicationview metrics from last completed replication").Default("true").Bool()
	DsmadmcReplicationViewsExec     = dsmadmcReplicationViews
	replicationviewCache            = map[string]map[string]ReplicationViewMetric{}
	replicationviewCacheMutex       = sync.RWMutex{}
	replicationviewMetricCache      = map[string]ReplicationViewMetric{}
	replicationviewMetricCacheMutex = sync.RWMutex{}
	replMap                         = map[string]string{
		"START_TIME":          "StartTime",
		"END_TIME":            "EndTime",
		"NODE_NAME":           "NodeName",
		"FSNAME":              "FsName",
		"COMP_STATE":          "CompState",
		"TOTFILES_REPLICATED": "ReplicatedFiles",
		"TOTBYTES_REPLICATED": "ReplicatedBytes",
	}
)

type ReplicationViewMetric struct {
	NodeName        string
	FsName          string
	StartTime       string
	EndTime         string
	CompState       string
	StartTimestamp  float64
	EndTimestamp    float64
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
	useCache        bool
}

func init() {
	registerCollector("replicationview", true, NewReplicationViewsExporter)
}

func NewReplicationViewsExporter(target *config.Target, logger log.Logger, useCache bool) Collector {
	return &ReplicationViewsCollector{
		NotCompleted: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "not_completed"),
			"Number of replications not completed for today", []string{"nodename", "fsname"}, nil),
		StartTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "start_time"),
			"Start time of replication", []string{"nodename", "fsname"}, nil),
		EndTime: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "end_time"),
			"End time of replication", []string{"nodename", "fsname"}, nil),
		Duration: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "duration_seconds"),
			"Amount of time taken to complete the most recent replication", []string{"nodename", "fsname"}, nil),
		ReplicatedBytes: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "replicated_bytes"),
			"Amount of data replicated in bytes", []string{"nodename", "fsname"}, nil),
		ReplicatedFiles: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "replicated_files"),
			"Number of files replicated", []string{"nodename", "fsname"}, nil),
		target:   target,
		logger:   logger,
		useCache: useCache,
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
		ch <- prometheus.MustNewConstMetric(c.NotCompleted, prometheus.GaugeValue, m.NotCompleted, m.NodeName, m.FsName)
		ch <- prometheus.MustNewConstMetric(c.StartTime, prometheus.GaugeValue, m.StartTimestamp, m.NodeName, m.FsName)
		ch <- prometheus.MustNewConstMetric(c.EndTime, prometheus.GaugeValue, m.EndTimestamp, m.NodeName, m.FsName)
		ch <- prometheus.MustNewConstMetric(c.Duration, prometheus.GaugeValue, m.Duration, m.NodeName, m.FsName)
		ch <- prometheus.MustNewConstMetric(c.ReplicatedBytes, prometheus.GaugeValue, m.ReplicatedBytes, m.NodeName, m.FsName)
		ch <- prometheus.MustNewConstMetric(c.ReplicatedFiles, prometheus.GaugeValue, m.ReplicatedFiles, m.NodeName, m.FsName)
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "replicationview")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "replicationview")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "replicationview")
}

func (c *ReplicationViewsCollector) collect() (map[string]ReplicationViewMetric, error) {
	var err error
	var out string
	metrics := make(map[string]ReplicationViewMetric)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*replicationviewTimeout)*time.Second)
	defer cancel()
	out, err = DsmadmcReplicationViewsExec(c.target, ctx, c.logger)
	if err != nil {
		if c.useCache {
			metrics = replicationviewReadCache(c.target.Name)
		}
		return metrics, err
	}
	metrics = replicationviewParse(out, c.target, *useReplicationViewMetricCache, c.logger)
	if c.useCache {
		replicationviewWriteCache(c.target.Name, metrics)
	}
	return metrics, nil
}

func dsmadmcReplicationViews(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	fields := getReplFields()
	query := fmt.Sprintf("SELECT %s FROM replicationview", strings.Join(fields, ","))
	now := time.Now().Local()
	today := now.Format("2006-01-02")
	yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")
	query = query + fmt.Sprintf(" WHERE DATE(START_TIME) BETWEEN '%s' AND '%s'", yesterday, today)
	query = query + " order by START_TIME desc"
	out, err := dsmadmcQuery(target, query, ctx, logger)
	return out, err
}

func replicationviewParse(out string, target *config.Target, useCache bool, logger log.Logger) map[string]ReplicationViewMetric {
	metrics := make(map[string]ReplicationViewMetric)
	notCompleted := make(map[string]float64)
	fields := getReplFields()
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		values := strings.Split(strings.TrimSpace(line), ",")
		if len(values) != len(fields) {
			continue
		}
		var metric ReplicationViewMetric
		ps := reflect.ValueOf(&metric) // pointer to struct - addressable
		s := ps.Elem()                 //struct
		for i, k := range fields {
			field := replMap[k]
			f := s.FieldByName(field)
			if f.Kind() == reflect.String {
				f.SetString(values[i])
			} else if f.Kind() == reflect.Float64 {
				val, err := strconv.ParseFloat(values[i], 64)
				if err != nil {
					level.Error(logger).Log("msg", fmt.Sprintf("Error parsing %s value %s: %s", k, values[i], err.Error()))
					continue
				} else {
					f.SetFloat(val)
				}
			}
		}
		if target.ReplicationNodeNames != nil && !sliceContains(target.ReplicationNodeNames, metric.NodeName) {
			continue
		}
		key := fmt.Sprintf("%s-%s", metric.NodeName, metric.FsName)
		if metric.CompState != "COMPLETE" {
			notCompleted[key]++
		}
		if _, ok := metrics[key]; ok {
			continue
		} else {
			metrics[key] = metric
		}
	}
	for key, metric := range metrics {
		cacheKey := fmt.Sprintf("%s-%s", target.Name, key)
		if val, ok := notCompleted[key]; ok {
			metric.NotCompleted = val
		}
		if metric.CompState != "COMPLETE" {
			if useCache {
				replicationviewMetricCacheMutex.RLock()
				if d, ok := replicationviewMetricCache[cacheKey]; ok {
					metric.Duration = d.Duration
					metric.StartTimestamp = d.StartTimestamp
					metric.EndTimestamp = d.EndTimestamp
					metric.ReplicatedBytes = d.ReplicatedBytes
					metric.ReplicatedFiles = d.ReplicatedFiles
				}
				replicationviewMetricCacheMutex.RUnlock()
			}
		} else {
			start_time, err := time.Parse(timeFormat, metric.StartTime)
			if err != nil {
				level.Error(logger).Log("msg", fmt.Sprintf("Failed to parse START_TIME '%v': %s", metric.StartTime, err.Error()))
				continue
			} else {
				metric.StartTimestamp = float64(start_time.Unix())
			}
			end_time, err := time.Parse(timeFormat, metric.EndTime)
			if err != nil {
				level.Error(logger).Log("msg", fmt.Sprintf("Failed to parse END_TIME '%v': %s", metric.EndTime, err.Error()))
				continue
			} else {
				metric.EndTimestamp = float64(end_time.Unix())
			}
			if metric.EndTimestamp < 0 {
				metric.EndTimestamp = 0
				metric.Duration = 0
			} else {
				duration := end_time.Sub(start_time).Seconds()
				metric.Duration = duration
			}
			if useCache {
				replicationviewMetricCacheMutex.Lock()
				replicationviewMetricCache[cacheKey] = metric
				replicationviewMetricCacheMutex.Unlock()
			}
		}
		metrics[key] = metric
	}
	return metrics
}

func getReplFields() []string {
	var fields []string
	for k := range replMap {
		fields = append(fields, k)
	}
	sort.Strings(fields)
	return fields
}

func replicationviewReadCache(target string) map[string]ReplicationViewMetric {
	metrics := make(map[string]ReplicationViewMetric)
	replicationviewCacheMutex.RLock()
	if cache, ok := replicationviewCache[target]; ok {
		metrics = cache
	}
	replicationviewCacheMutex.RUnlock()
	return metrics
}

func replicationviewWriteCache(target string, metrics map[string]ReplicationViewMetric) {
	replicationviewCacheMutex.Lock()
	replicationviewCache[target] = metrics
	replicationviewCacheMutex.Unlock()
}
