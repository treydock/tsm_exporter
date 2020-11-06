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
	occupancyTimeout      = kingpin.Flag("collector.occupancy.timeout", "Timeout for collecting occupancy information").Default("10").Int()
	DsmadmcOccupancysExec = dsmadmcOccupancys
	occupancyCache        = map[string][]OccupancyMetric{}
	occupancyCacheMutex   = sync.RWMutex{}
	occupancyMap          = map[string]string{
		"NODE_NAME":      "NodeName",
		"FILESPACE_NAME": "FilespaceName",
		"STGPOOL_NAME":   "StoragePoolName",
		"NUM_FILES":      "Files",
		"PHYSICAL_MB":    "Physical",
		"LOGICAL_MB":     "Logical",
	}
	occupancyLabelFields = []string{"NODE_NAME", "FILESPACE_NAME", "STGPOOL_NAME"}
)

type OccupancyMetric struct {
	NodeName        string
	FilespaceName   string
	StoragePoolName string
	Files           float64
	Physical        float64
	Logical         float64
}

type OccupancysCollector struct {
	physical *prometheus.Desc
	logical  *prometheus.Desc
	files    *prometheus.Desc
	target   *config.Target
	logger   log.Logger
	useCache bool
}

func init() {
	registerCollector("occupancy", true, NewOccupancysExporter)
}

func NewOccupancysExporter(target *config.Target, logger log.Logger, useCache bool) Collector {
	return &OccupancysCollector{
		physical: prometheus.NewDesc(prometheus.BuildFQName(namespace, "occupancy", "physical_bytes"),
			"Physical space occupied", []string{"nodename", "filespace", "storagepool"}, nil),
		logical: prometheus.NewDesc(prometheus.BuildFQName(namespace, "occupancy", "logical_bytes"),
			"Logical space occupied", []string{"nodename", "filespace", "storagepool"}, nil),
		files: prometheus.NewDesc(prometheus.BuildFQName(namespace, "occupancy", "files"),
			"Number of files", []string{"nodename", "filespace", "storagepool"}, nil),
		target:   target,
		logger:   logger,
		useCache: useCache,
	}
}

func (c *OccupancysCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.physical
	ch <- c.logical
	ch <- c.files
}

func (c *OccupancysCollector) Collect(ch chan<- prometheus.Metric) {
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
		ch <- prometheus.MustNewConstMetric(c.physical, prometheus.GaugeValue, m.Physical, m.NodeName, m.FilespaceName, m.StoragePoolName)
		ch <- prometheus.MustNewConstMetric(c.logical, prometheus.GaugeValue, m.Logical, m.NodeName, m.FilespaceName, m.StoragePoolName)
		ch <- prometheus.MustNewConstMetric(c.files, prometheus.GaugeValue, m.Files, m.NodeName, m.FilespaceName, m.StoragePoolName)
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "occupancy")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "occupancy")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "occupancy")
}

func (c *OccupancysCollector) collect() ([]OccupancyMetric, error) {
	var err error
	var out string
	var metrics []OccupancyMetric
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*occupancyTimeout)*time.Second)
	defer cancel()
	out, err = DsmadmcOccupancysExec(c.target, ctx, c.logger)
	if err != nil {
		if c.useCache {
			metrics = occupancyReadCache(c.target.Name)
		}
		return metrics, err
	}
	metrics = occupancyParse(out, c.logger)
	if c.useCache {
		occupancyWriteCache(c.target.Name, metrics)
	}
	return metrics, nil
}

func dsmadmcOccupancys(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	fields := getOccupancyFields()
	var queryFields []string
	var groupFields []string
	for _, f := range fields {
		var field string
		if sliceContains(occupancyLabelFields, f) {
			groupFields = append(groupFields, f)
			field = f
		} else {
			field = fmt.Sprintf("SUM(%s)", f)
		}
		queryFields = append(queryFields, field)
	}
	query := fmt.Sprintf("SELECT %s FROM occupancy GROUP BY %s", strings.Join(queryFields, ","), strings.Join(groupFields, ","))
	out, err := dsmadmcQuery(target, query, ctx, logger)
	return out, err
}

func occupancyParse(out string, logger log.Logger) []OccupancyMetric {
	var metrics []OccupancyMetric
	fields := getOccupancyFields()
	lines := strings.Split(out, "\n")
	for _, l := range lines {
		values := strings.Split(strings.TrimSpace(l), ",")
		if len(values) != len(fields) {
			continue
		}
		var metric OccupancyMetric
		ps := reflect.ValueOf(&metric) // pointer to struct - addressable
		s := ps.Elem()                 //struct
		for i, k := range fields {
			field := occupancyMap[k]
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
					valBytes := val * 1024 * 1024
					f.SetFloat(valBytes)
				} else {
					f.SetFloat(val)
				}
			}
		}
		metrics = append(metrics, metric)
	}
	return metrics
}

func getOccupancyFields() []string {
	var fields []string
	for k := range occupancyMap {
		fields = append(fields, k)
	}
	sort.Strings(fields)
	return fields
}

func occupancyReadCache(target string) []OccupancyMetric {
	var metrics []OccupancyMetric
	occupancyCacheMutex.RLock()
	if cache, ok := occupancyCache[target]; ok {
		metrics = cache
	}
	occupancyCacheMutex.RUnlock()
	return metrics
}

func occupancyWriteCache(target string, metrics []OccupancyMetric) {
	occupancyCacheMutex.Lock()
	occupancyCache[target] = metrics
	occupancyCacheMutex.Unlock()
}
