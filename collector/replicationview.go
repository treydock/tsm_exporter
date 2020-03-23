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
	replicationviewsTimeout            = kingpin.Flag("collector.replicationviews.timeout", "Timeout for collecting replicationviews information").Default("5").Int()
	useReplicationViewDurationCache    = kingpin.Flag("collector.replicationviews.duration-cache", "Cache replicationview durations").Default("true").Bool()
	DsmadmcReplicationViewsExec        = dsmadmcReplicationViews
	replicationviewsCache              = map[string]map[string]ReplicationViewMetric{}
	replicationviewsCacheMutex         = sync.RWMutex{}
	replicationviewsDurationCache      = map[string]float64{}
	replicationviewsDurationCacheMutex = sync.RWMutex{}
	replMap                            = map[string]string{
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
	NotCompleted    float64
	Duration        float64
	ReplicatedBytes float64
	ReplicatedFiles float64
}

type ReplicationViewsCollector struct {
	NotCompleted    *prometheus.Desc
	Duration        *prometheus.Desc
	ReplicatedBytes *prometheus.Desc
	ReplicatedFiles *prometheus.Desc
	target          *config.Target
	logger          log.Logger
	useCache        bool
}

func init() {
	registerCollector("replicationviews", true, NewReplicationViewsExporter)
}

func NewReplicationViewsExporter(target *config.Target, logger log.Logger, useCache bool) Collector {
	return &ReplicationViewsCollector{
		NotCompleted: prometheus.NewDesc(prometheus.BuildFQName(namespace, "replication", "not_completed"),
			"Number of replications not completed for today", []string{"nodename", "fsname"}, nil),
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
	ch <- c.Duration
	ch <- c.ReplicatedBytes
	ch <- c.ReplicatedFiles
}

func (c *ReplicationViewsCollector) Collect(ch chan<- prometheus.Metric) {
	level.Debug(c.logger).Log("msg", "Collecting replicationviews metrics")
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
		ch <- prometheus.MustNewConstMetric(c.NotCompleted, prometheus.GaugeValue, m.NotCompleted, m.NodeName, m.FsName)
		ch <- prometheus.MustNewConstMetric(c.Duration, prometheus.GaugeValue, m.Duration, m.NodeName, m.FsName)
		ch <- prometheus.MustNewConstMetric(c.ReplicatedBytes, prometheus.GaugeValue, m.ReplicatedBytes, m.NodeName, m.FsName)
		ch <- prometheus.MustNewConstMetric(c.ReplicatedFiles, prometheus.GaugeValue, m.ReplicatedFiles, m.NodeName, m.FsName)
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "replicationviews")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "replicationviews")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "replicationviews")
}

func (c *ReplicationViewsCollector) collect() (map[string]ReplicationViewMetric, error) {
	var err error
	var out string
	metrics := make(map[string]ReplicationViewMetric)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*replicationviewsTimeout)*time.Second)
	defer cancel()
	out, err = DsmadmcReplicationViewsExec(c.target, ctx, c.logger)
	if ctx.Err() == context.DeadlineExceeded {
		if c.useCache {
			metrics = replicationviewsReadCache(c.target.Name)
		}
		return metrics, ctx.Err()
	}
	if err != nil {
		if c.useCache {
			metrics = replicationviewsReadCache(c.target.Name)
		}
		return metrics, err
	}
	metrics, err = replicationviewsParse(out, c.target, *useReplicationViewDurationCache, c.logger)
	if err != nil {
		if c.useCache {
			metrics = replicationviewsReadCache(c.target.Name)
		}
		return metrics, err
	}
	if c.useCache {
		replicationviewsWriteCache(c.target.Name, metrics)
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

func replicationviewsParse(out string, target *config.Target, useCache bool, logger log.Logger) (map[string]ReplicationViewMetric, error) {
	metrics := make(map[string]ReplicationViewMetric)
	fields := getReplFields()
	timeFormat := "2006-01-02 15:04:05.000000"
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
			} else {
				val, err := strconv.ParseFloat(values[i], 64)
				if err != nil {
					level.Error(logger).Log("msg", fmt.Sprintf("Error parsing %s value %s: %s", k, values[i], err.Error()))
					continue
				}
				if strings.HasSuffix(k, "_MB") {
					valBytes := val * 1024.0 * 1024.0
					f.SetFloat(valBytes)
				} else {
					f.SetFloat(val)
				}
			}
		}
		if target.ReplicationNodeNames != nil && !sliceContains(target.ReplicationNodeNames, metric.NodeName) {
			continue
		}
		key := fmt.Sprintf("%s-%s", metric.NodeName, metric.FsName)
		if _, ok := metrics[key]; ok {
			continue
		} else {
			metrics[key] = metric
		}
	}
	for key, metric := range metrics {
		durationCacheKey := fmt.Sprintf("%s-%s", target.Name, key)
		if metric.CompState != "COMPLETE" {
			metric.NotCompleted++
			if useCache {
				replicationviewsDurationCacheMutex.RLock()
				if d, ok := replicationviewsDurationCache[durationCacheKey]; ok {
					metric.Duration = d
				}
				replicationviewsDurationCacheMutex.RUnlock()
			}
		} else {
			start_time, err := time.Parse(timeFormat, metric.StartTime)
			if err != nil {
				level.Error(logger).Log("msg", fmt.Sprintf("Failed to parse actual_start time '%v': %s", metric.StartTime, err.Error()))
				continue
			}
			end_time, err := time.Parse(timeFormat, metric.EndTime)
			if err != nil {
				level.Error(logger).Log("msg", fmt.Sprintf("Failed to parse completed time '%v': %s", metric.EndTime, err.Error()))
				continue
			}
			duration := end_time.Sub(start_time).Seconds()
			metric.Duration = duration
			if useCache {
				replicationviewsDurationCacheMutex.Lock()
				replicationviewsDurationCache[durationCacheKey] = metric.Duration
				replicationviewsDurationCacheMutex.Unlock()
			}
		}
		metrics[key] = metric
	}
	return metrics, nil
}

func getReplFields() []string {
	var fields []string
	for k := range replMap {
		fields = append(fields, k)
	}
	sort.Strings(fields)
	return fields
}

func replicationviewsReadCache(target string) map[string]ReplicationViewMetric {
	metrics := make(map[string]ReplicationViewMetric)
	replicationviewsCacheMutex.RLock()
	if cache, ok := replicationviewsCache[target]; ok {
		metrics = cache
	}
	replicationviewsCacheMutex.RUnlock()
	return metrics
}

func replicationviewsWriteCache(target string, metrics map[string]ReplicationViewMetric) {
	replicationviewsCacheMutex.Lock()
	replicationviewsCache[target] = metrics
	replicationviewsCacheMutex.Unlock()
}
