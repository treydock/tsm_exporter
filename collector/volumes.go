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
	volumesTimeout            = kingpin.Flag("collector.volumes.timeout", "Timeout for collecting volumes information").Default("5").Int()
	DsmadmcVolumesUnavailExec = dsmadmcVolumesUnavail
	volumesCache              = map[string]VolumeUnavailMetric{}
	volumesCacheMutex         = sync.RWMutex{}
)

type VolumeUnavailMetric struct {
	unavailable float64
}

type VolumesCollector struct {
	unavailable *prometheus.Desc
	target      config.Target
	logger      log.Logger
	useCache    bool
}

func init() {
	registerCollector("volumes", true, NewVolumesExporter)
}

func NewVolumesExporter(target config.Target, logger log.Logger, useCache bool) Collector {
	return &VolumesCollector{
		unavailable: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volumes", "unavailable"),
			"Number of unavailable volumes", nil, nil),
		target:   target,
		logger:   logger,
		useCache: useCache,
	}
}

func (c *VolumesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.unavailable
}

func (c *VolumesCollector) Collect(ch chan<- prometheus.Metric) {
	level.Debug(c.logger).Log("msg", "Collecting volumes metrics")
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

	if err == nil || c.useCache {
		ch <- prometheus.MustNewConstMetric(c.unavailable, prometheus.GaugeValue, metrics.unavailable)
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "volumes")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "volumes")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "volumes")
}

func (c *VolumesCollector) collect() (VolumeUnavailMetric, error) {
	var err error
	var out string
	var metrics VolumeUnavailMetric
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*volumesTimeout)*time.Second)
	defer cancel()
	out, err = DsmadmcVolumesUnavailExec(c.target, ctx, c.logger)
	if ctx.Err() == context.DeadlineExceeded {
		if c.useCache {
			metrics = volumesReadCache(c.target.Name)
		}
		return metrics, ctx.Err()
	}
	if err != nil {
		if c.useCache {
			metrics = volumesReadCache(c.target.Name)
		}
		return metrics, err
	}
	metrics, err = volumesUnavailParse(out, c.logger)
	if err != nil {
		if c.useCache {
			metrics = volumesReadCache(c.target.Name)
		}
		return metrics, err
	}
	if c.useCache {
		volumesWriteCache(c.target.Name, metrics)
	}
	return metrics, nil
}

func dsmadmcVolumesUnavail(target config.Target, ctx context.Context, logger log.Logger) (string, error) {
	query := "SELECT count(*) FROM volumes WHERE access='UNAVAILABLE'"
	out, err := dsmadmcQuery(target, query, ctx, logger)
	return out, err
}

func volumesUnavailParse(out string, logger log.Logger) (VolumeUnavailMetric, error) {
	var metric VolumeUnavailMetric
	lines := strings.Split(out, "\n")
	for _, l := range lines {
		if val, err := strconv.ParseFloat(strings.TrimSpace(l), 64); err != nil {
			continue
		} else {
			metric.unavailable = val
			break
		}
	}
	return metric, nil
}

func volumesReadCache(target string) VolumeUnavailMetric {
	var metrics VolumeUnavailMetric
	volumesCacheMutex.RLock()
	if cache, ok := volumesCache[target]; ok {
		metrics = cache
	}
	volumesCacheMutex.RUnlock()
	return metrics
}

func volumesWriteCache(target string, metrics VolumeUnavailMetric) {
	volumesCacheMutex.Lock()
	volumesCache[target] = metrics
	volumesCacheMutex.Unlock()
}
