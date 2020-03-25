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
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treydock/tsm_exporter/config"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	drivesTimeout     = kingpin.Flag("collector.drives.timeout", "Timeout for collecting drives information").Default("5").Int()
	DsmadmcDrivesExec = dsmadmcDrives
	drivesCache       = map[string][]DriveMetric{}
	drivesCacheMutex  = sync.RWMutex{}
)

type DriveMetric struct {
	library string
	name    string
	online  bool
}

type DrivesCollector struct {
	online   *prometheus.Desc
	target   *config.Target
	logger   log.Logger
	useCache bool
}

func init() {
	registerCollector("drives", true, NewDrivesExporter)
}

func NewDrivesExporter(target *config.Target, logger log.Logger, useCache bool) Collector {
	return &DrivesCollector{
		online: prometheus.NewDesc(prometheus.BuildFQName(namespace, "drive", "online"),
			"Inidicates if the drive is online, 1=online, 0=offline", []string{"library", "name"}, nil),
		target:   target,
		logger:   logger,
		useCache: useCache,
	}
}

func (c *DrivesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.online
}

func (c *DrivesCollector) Collect(ch chan<- prometheus.Metric) {
	level.Debug(c.logger).Log("msg", "Collecting drives metrics")
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
		ch <- prometheus.MustNewConstMetric(c.online, prometheus.GaugeValue, boolToFloat64(m.online), m.library, m.name)
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "drives")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "drives")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "drives")
}

func (c *DrivesCollector) collect() ([]DriveMetric, error) {
	var err error
	var out string
	var metrics []DriveMetric
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*drivesTimeout)*time.Second)
	defer cancel()
	out, err = DsmadmcDrivesExec(c.target, ctx, c.logger)
	if ctx.Err() == context.DeadlineExceeded {
		if c.useCache {
			metrics = drivesReadCache(c.target.Name)
		}
		return metrics, ctx.Err()
	}
	if err != nil {
		if c.useCache {
			metrics = drivesReadCache(c.target.Name)
		}
		return metrics, err
	}
	metrics, err = drivesParse(out, c.logger)
	if err != nil {
		if c.useCache {
			metrics = drivesReadCache(c.target.Name)
		}
		return metrics, err
	}
	if c.useCache {
		drivesWriteCache(c.target.Name, metrics)
	}
	return metrics, nil
}

func dsmadmcDrives(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	query := "SELECT library_name,drive_name,online FROM drives"
	if target.LibraryName != "" {
		query = query + fmt.Sprintf(" WHERE library_name='%s'", target.LibraryName)
	}
	out, err := dsmadmcQuery(target, query, ctx, logger)
	return out, err
}

func drivesParse(out string, logger log.Logger) ([]DriveMetric, error) {
	var metrics []DriveMetric
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		l := strings.TrimSpace(line)
		items := strings.Split(l, ",")
		if len(items) != 3 {
			continue
		}
		var metric DriveMetric
		metric.library = items[0]
		metric.name = items[1]
		if items[2] == "YES" {
			metric.online = true
		} else {
			metric.online = false
		}
		metrics = append(metrics, metric)
	}
	return metrics, nil
}

func drivesReadCache(target string) []DriveMetric {
	var metrics []DriveMetric
	drivesCacheMutex.RLock()
	if cache, ok := drivesCache[target]; ok {
		metrics = cache
	}
	drivesCacheMutex.RUnlock()
	return metrics
}

func drivesWriteCache(target string, metrics []DriveMetric) {
	drivesCacheMutex.Lock()
	drivesCache[target] = metrics
	drivesCacheMutex.Unlock()
}
