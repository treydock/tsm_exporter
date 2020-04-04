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
	volumesTimeout     = kingpin.Flag("collector.volumes.timeout", "Timeout for collecting volumes information").Default("5").Int()
	DsmadmcVolumesExec = dsmadmcVolumes
	volumesCache       = map[string]VolumeMetric{}
	volumesCacheMutex  = sync.RWMutex{}
)

type VolumeMetric struct {
	unavailable float64
	readonly    float64
}

type VolumesCollector struct {
	unavailable *prometheus.Desc
	readonly    *prometheus.Desc
	target      *config.Target
	logger      log.Logger
	useCache    bool
}

func init() {
	registerCollector("volumes", true, NewVolumesExporter)
}

func NewVolumesExporter(target *config.Target, logger log.Logger, useCache bool) Collector {
	return &VolumesCollector{
		unavailable: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volumes", "unavailable"),
			"Number of unavailable volumes", nil, nil),
		readonly: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volumes", "readonly"),
			"Number of readonly volumes", nil, nil),
		target:   target,
		logger:   logger,
		useCache: useCache,
	}
}

func (c *VolumesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.unavailable
	ch <- c.readonly
}

func (c *VolumesCollector) Collect(ch chan<- prometheus.Metric) {
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

	if err == nil || c.useCache {
		ch <- prometheus.MustNewConstMetric(c.unavailable, prometheus.GaugeValue, metrics.unavailable)
		ch <- prometheus.MustNewConstMetric(c.readonly, prometheus.GaugeValue, metrics.readonly)
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "volumes")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "volumes")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "volumes")
}

func (c *VolumesCollector) collect() (VolumeMetric, error) {
	var err error
	var out string
	var metrics VolumeMetric
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*volumesTimeout)*time.Second)
	defer cancel()
	out, err = DsmadmcVolumesExec(c.target, ctx, c.logger)
	if err != nil {
		if c.useCache {
			metrics = volumesReadCache(c.target.Name)
		}
		return metrics, err
	}
	metrics = volumesParse(out, c.logger)
	if c.useCache {
		volumesWriteCache(c.target.Name, metrics)
	}
	return metrics, nil
}

func dsmadmcVolumes(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	query := "SELECT access FROM volumes WHERE access='UNAVAILABLE' OR access='READONLY'"
	out, err := dsmadmcQuery(target, query, ctx, logger)
	return out, err
}

func volumesParse(out string, logger log.Logger) VolumeMetric {
	var metric VolumeMetric
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		l := strings.TrimSpace(line)
		switch l {
		case "UNAVAILABLE":
			metric.unavailable++
		case "READONLY":
			metric.readonly++
		}
	}
	return metric
}

func volumesReadCache(target string) VolumeMetric {
	var metrics VolumeMetric
	volumesCacheMutex.RLock()
	if cache, ok := volumesCache[target]; ok {
		metrics = cache
	}
	volumesCacheMutex.RUnlock()
	return metrics
}

func volumesWriteCache(target string, metrics VolumeMetric) {
	volumesCacheMutex.Lock()
	volumesCache[target] = metrics
	volumesCacheMutex.Unlock()
}
