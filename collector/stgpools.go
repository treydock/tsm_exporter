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
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treydock/tsm_exporter/config"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	stgpoolsTimeout        = kingpin.Flag("collector.stgpools.timeout", "Timeout for collecting stgpools information").Default("10").Int()
	DsmadmcStoragePoolExec = dsmadmcStoragePool
	stgpoolsMap            = map[string]string{
		"STGPOOL_NAME": "Name",
		"POOLTYPE":     "PoolType",
		"DEVCLASS":     "ClassName",
		"STG_TYPE":     "StorageType",
		"PCT_UTILIZED": "PercentUtilized",
	}
)

type StoragePoolMetric struct {
	Name            string
	PoolType        string
	ClassName       string
	StorageType     string
	PercentUtilized float64
}

type StoragePoolCollector struct {
	PercentUtilized *prometheus.Desc
	target          *config.Target
	logger          log.Logger
}

func init() {
	registerCollector("stgpools", true, NewStoragePoolExporter)
}

func NewStoragePoolExporter(target *config.Target, logger log.Logger) Collector {
	return &StoragePoolCollector{
		PercentUtilized: prometheus.NewDesc(prometheus.BuildFQName(namespace, "storage_pool", "utilized_percent"),
			"Storage pool utilized percent", []string{"storagepool", "pooltype", "classname", "storagetype"}, nil),
		target: target,
		logger: logger,
	}
}

func (c *StoragePoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.PercentUtilized
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
		ch <- prometheus.MustNewConstMetric(c.PercentUtilized, prometheus.GaugeValue, m.PercentUtilized, m.Name, m.PoolType, m.ClassName, m.StorageType)
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
	metrics := stgpoolsParse(out, c.logger)
	return metrics, nil
}

func dsmadmcStoragePool(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	fields := getStoragePoolFields()
	query := fmt.Sprintf("SELECT %s FROM stgpools", strings.Join(fields, ","))
	out, err := dsmadmcQuery(target, query, ctx, logger)
	return out, err
}

func stgpoolsParse(out string, logger log.Logger) []StoragePoolMetric {
	var metrics []StoragePoolMetric
	fields := getStoragePoolFields()
	lines := strings.Split(out, "\n")
	for _, l := range lines {
		values := strings.Split(strings.TrimSpace(l), ",")
		if len(values) != len(fields) {
			continue
		}
		var metric StoragePoolMetric
		ps := reflect.ValueOf(&metric) // pointer to struct - addressable
		s := ps.Elem()                 //struct
		for i, k := range fields {
			field := stgpoolsMap[k]
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

func getStoragePoolFields() []string {
	var fields []string
	for k := range stgpoolsMap {
		fields = append(fields, k)
	}
	sort.Strings(fields)
	return fields
}
