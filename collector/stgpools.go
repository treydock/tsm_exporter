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
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treydock/tsm_exporter/config"
)

var (
	stgpoolsTimeout        = kingpin.Flag("collector.stgpools.timeout", "Timeout for collecting stgpools information").Default("10").Int()
	DsmadmcStoragePoolExec = dsmadmcStoragePool
	stgpoolsMap            = map[string]string{
		"STGPOOL_NAME":          "Name",
		"POOLTYPE":              "PoolType",
		"DEVCLASS":              "ClassName",
		"STG_TYPE":              "StorageType",
		"PCT_LOGICAL":           "PercentLogical",
		"PCT_UTILIZED":          "PercentUtilized",
		"EST_CAPACITY_MB":       "EstimatedCapacity",
		"TOTAL_CLOUD_SPACE_MB":  "TotalCloudSpace",
		"USED_CLOUD_SPACE_MB":   "UsedCloudSpace",
		"LOCAL_EST_CAPACITY_MB": "LocalEstimatedCapacity",
		"LOCAL_PCT_LOGICAL":     "LocalPercentLogical",
		"LOCAL_PCT_UTILIZED":    "LocalPercentUtilized",
	}
)

type StoragePoolMetric struct {
	Name                   string
	PoolType               string
	ClassName              string
	StorageType            string
	PercentLogical         float64
	PercentUtilized        float64
	EstimatedCapacity      float64
	TotalCloudSpace        float64
	UsedCloudSpace         float64
	LocalEstimatedCapacity float64
	LocalPercentLogical    float64
	LocalPercentUtilized   float64
}

type StoragePoolCollector struct {
	PercentLogical         *prometheus.Desc
	PercentUtilized        *prometheus.Desc
	EstimatedCapacity      *prometheus.Desc
	TotalCloudSpace        *prometheus.Desc
	UsedCloudSpace         *prometheus.Desc
	LocalEstimatedCapacity *prometheus.Desc
	LocalPercentLogical    *prometheus.Desc
	LocalPercentUtilized   *prometheus.Desc
	target                 *config.Target
	logger                 log.Logger
}

func init() {
	registerCollector("stgpools", true, NewStoragePoolExporter)
}

func NewStoragePoolExporter(target *config.Target, logger log.Logger) Collector {
	labels := []string{"storagepool", "pooltype", "classname", "storagetype"}
	return &StoragePoolCollector{
		PercentLogical: prometheus.NewDesc(prometheus.BuildFQName(namespace, "storage_pool", "logical_ratio"),
			"Storage pool logical occupancy ratio, 0.0-1.0", labels, nil),
		PercentUtilized: prometheus.NewDesc(prometheus.BuildFQName(namespace, "storage_pool", "utilized_ratio"),
			"Storage pool utilized ratio, 0.0-1.0", labels, nil),
		EstimatedCapacity: prometheus.NewDesc(prometheus.BuildFQName(namespace, "storage_pool", "estimated_capacity_bytes"),
			"Storage pool estimated capacity", labels, nil),
		TotalCloudSpace: prometheus.NewDesc(prometheus.BuildFQName(namespace, "storage_pool", "cloud_total_bytes"),
			"Storage pool total cloud space", labels, nil),
		UsedCloudSpace: prometheus.NewDesc(prometheus.BuildFQName(namespace, "storage_pool", "cloud_used_bytes"),
			"Storage pool used cloud space", labels, nil),
		LocalEstimatedCapacity: prometheus.NewDesc(prometheus.BuildFQName(namespace, "storage_pool", "local_estimated_capacity_bytes"),
			"Storage pool local estimated capacity", labels, nil),
		LocalPercentLogical: prometheus.NewDesc(prometheus.BuildFQName(namespace, "storage_pool", "local_logical_ratio"),
			"Storage pool local logical occupancy ratio, 0.0-1.0", labels, nil),
		LocalPercentUtilized: prometheus.NewDesc(prometheus.BuildFQName(namespace, "storage_pool", "local_utilized_ratio"),
			"Storage pool local utilized ratio, 0.0-1.0", labels, nil),
		target: target,
		logger: logger,
	}
}

func (c *StoragePoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.PercentLogical
	ch <- c.PercentUtilized
	ch <- c.EstimatedCapacity
	ch <- c.TotalCloudSpace
	ch <- c.UsedCloudSpace
	ch <- c.LocalEstimatedCapacity
	ch <- c.LocalPercentLogical
	ch <- c.LocalPercentUtilized
}

func (c *StoragePoolCollector) Collect(ch chan<- prometheus.Metric) {
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
		labels := []string{m.Name, m.PoolType, m.ClassName, m.StorageType}
		if !math.IsNaN(m.PercentLogical) {
			ch <- prometheus.MustNewConstMetric(c.PercentLogical, prometheus.GaugeValue, m.PercentLogical, labels...)
		}
		if !math.IsNaN(m.PercentUtilized) {
			ch <- prometheus.MustNewConstMetric(c.PercentUtilized, prometheus.GaugeValue, m.PercentUtilized, labels...)
		}
		if !math.IsNaN(m.EstimatedCapacity) {
			ch <- prometheus.MustNewConstMetric(c.EstimatedCapacity, prometheus.GaugeValue, m.EstimatedCapacity, labels...)
		}
		if !math.IsNaN(m.TotalCloudSpace) {
			ch <- prometheus.MustNewConstMetric(c.TotalCloudSpace, prometheus.GaugeValue, m.TotalCloudSpace, labels...)
		}
		if !math.IsNaN(m.UsedCloudSpace) {
			ch <- prometheus.MustNewConstMetric(c.UsedCloudSpace, prometheus.GaugeValue, m.UsedCloudSpace, labels...)
		}
		if !math.IsNaN(m.LocalEstimatedCapacity) {
			ch <- prometheus.MustNewConstMetric(c.LocalEstimatedCapacity, prometheus.GaugeValue, m.LocalEstimatedCapacity, labels...)
		}
		if !math.IsNaN(m.LocalPercentLogical) {
			ch <- prometheus.MustNewConstMetric(c.LocalPercentLogical, prometheus.GaugeValue, m.LocalPercentLogical, labels...)
		}
		if !math.IsNaN(m.LocalPercentUtilized) {
			ch <- prometheus.MustNewConstMetric(c.LocalPercentUtilized, prometheus.GaugeValue, m.LocalPercentUtilized, labels...)
		}
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "stgpools")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "stgpools")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "stgpools")
}

func (c *StoragePoolCollector) collect() ([]StoragePoolMetric, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*stgpoolsTimeout)*time.Second)
	defer cancel()
	out, err := DsmadmcStoragePoolExec(c.target, ctx, c.logger)
	if err != nil {
		return nil, err
	}
	metrics, err := stgpoolsParse(out, c.logger)
	return metrics, err
}

func dsmadmcStoragePool(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	fields := getStoragePoolFields()
	query := fmt.Sprintf("SELECT %s FROM stgpools", strings.Join(fields, ","))
	out, err := dsmadmcQuery(target, query, ctx, logger)
	return out, err
}

func stgpoolsParse(out string, logger log.Logger) ([]StoragePoolMetric, error) {
	var metrics []StoragePoolMetric
	fields := getStoragePoolFields()
	records, err := getRecords(out, logger)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if len(record) != len(fields) {
			continue
		}
		var metric StoragePoolMetric
		ps := reflect.ValueOf(&metric) // pointer to struct - addressable
		s := ps.Elem()                 //struct
		for i, k := range fields {
			field := stgpoolsMap[k]
			f := s.FieldByName(field)
			if f.Kind() == reflect.String {
				f.SetString(record[i])
			} else {
				val, err := parseFloat(record[i])
				if err != nil {
					level.Error(logger).Log("msg", "Error parsing value", "key", k, "value", record[i], "record", strings.Join(record, ","), "err", err)
					return nil, err
				}
				if strings.Contains(field, "Percent") {
					val = val / 100
				} else if strings.HasSuffix(k, "_MB") {
					val = val * 1024 * 1024
				}
				f.SetFloat(val)
			}
		}
		metrics = append(metrics, metric)
	}
	return metrics, nil
}

func getStoragePoolFields() []string {
	var fields []string
	for k := range stgpoolsMap {
		fields = append(fields, k)
	}
	sort.Strings(fields)
	return fields
}
