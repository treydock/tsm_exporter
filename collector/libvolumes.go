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
	libvolumesTimeout     = kingpin.Flag("collector.libvolumes.timeout", "Timeout for collecting libvolumes information").Default("5").Int()
	DsmadmcLibVolumesExec = dsmadmcLibVolumes
	libvolumesCache       = map[string][]LibVolumeMetric{}
	libvolumesCacheMutex  = sync.RWMutex{}
)

type LibVolumeMetric struct {
	mediatype string
	status    string
	count     float64
}

type LibVolumesCollector struct {
	scratch  *prometheus.Desc
	media    *prometheus.Desc
	target   *config.Target
	logger   log.Logger
	useCache bool
}

func init() {
	registerCollector("libvolumes", true, NewLibVolumesExporter)
}

func NewLibVolumesExporter(target *config.Target, logger log.Logger, useCache bool) Collector {
	return &LibVolumesCollector{
		scratch: prometheus.NewDesc(prometheus.BuildFQName(namespace, "libvolume", "scratch"),
			"Number of scratch tapes", nil, nil),
		media: prometheus.NewDesc(prometheus.BuildFQName(namespace, "libvolume", "media"),
			"Number of tapes", []string{"mediatype", "status"}, nil),
		target:   target,
		logger:   logger,
		useCache: useCache,
	}
}

func (c *LibVolumesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.scratch
	ch <- c.media
}

func (c *LibVolumesCollector) Collect(ch chan<- prometheus.Metric) {
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

	var scratch float64
	for _, m := range metrics {
		if m.status == "scratch" {
			scratch += m.count
		}
		ch <- prometheus.MustNewConstMetric(c.media, prometheus.GaugeValue, m.count, m.mediatype, m.status)
	}
	if err == nil || c.useCache {
		ch <- prometheus.MustNewConstMetric(c.scratch, prometheus.GaugeValue, scratch)
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "libvolumes")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "libvolumes")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "libvolumes")
}

func (c *LibVolumesCollector) collect() ([]LibVolumeMetric, error) {
	var err error
	var out string
	var metrics []LibVolumeMetric
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*libvolumesTimeout)*time.Second)
	defer cancel()
	out, err = DsmadmcLibVolumesExec(c.target, ctx, c.logger)
	if err != nil {
		if c.useCache {
			metrics = libvolumesReadCache(c.target.Name)
		}
		return metrics, err
	}
	metrics = libvolumesParse(out, c.logger)
	if c.useCache {
		libvolumesWriteCache(c.target.Name, metrics)
	}
	return metrics, nil
}

func dsmadmcLibVolumes(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	query := "SELECT MEDIATYPE,STATUS,COUNT(*) FROM libvolumes GROUP BY(MEDIATYPE,STATUS)"
	if target.LibraryName != "" {
		query = query + fmt.Sprintf(" AND library_name='%s'", target.LibraryName)
	}
	out, err := dsmadmcQuery(target, query, ctx, logger)
	return out, err
}

func libvolumesParse(out string, logger log.Logger) []LibVolumeMetric {
	var metrics []LibVolumeMetric
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		l := strings.TrimSpace(line)
		items := strings.Split(l, ",")
		if len(items) != 3 {
			continue
		}
		var metric LibVolumeMetric
		metric.mediatype = items[0]
		metric.status = strings.ToLower(items[1])
		count, err := strconv.ParseFloat(items[2], 64)
		if err != nil {
			level.Error(logger).Log("msg", fmt.Sprintf("Error parsing libvolume value '%s': %s", items[2], err.Error()))
			continue
		}
		metric.count = count
		metrics = append(metrics, metric)
	}
	return metrics
}

func libvolumesReadCache(target string) []LibVolumeMetric {
	var metrics []LibVolumeMetric
	libvolumesCacheMutex.RLock()
	if cache, ok := libvolumesCache[target]; ok {
		metrics = cache
	}
	libvolumesCacheMutex.RUnlock()
	return metrics
}

func libvolumesWriteCache(target string, metrics []LibVolumeMetric) {
	libvolumesCacheMutex.Lock()
	libvolumesCache[target] = metrics
	libvolumesCacheMutex.Unlock()
}
