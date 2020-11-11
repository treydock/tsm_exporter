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
	"fmt"
	"strings"
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
)

type DriveMetric struct {
	library string
	name    string
	online  bool
	state   string
	volume  string
}

type DrivesCollector struct {
	online *prometheus.Desc
	state  *prometheus.Desc
	volume *prometheus.Desc
	target *config.Target
	logger log.Logger
}

func init() {
	registerCollector("drives", true, NewDrivesExporter)
}

func NewDrivesExporter(target *config.Target, logger log.Logger) Collector {
	return &DrivesCollector{
		online: prometheus.NewDesc(prometheus.BuildFQName(namespace, "drive", "online"),
			"Inidicates if the drive is online, 1=online, 0=offline", []string{"library", "drive"}, nil),
		state: prometheus.NewDesc(prometheus.BuildFQName(namespace, "drive", "state_info"),
			"Current state of the drive", []string{"library", "drive", "state"}, nil),
		volume: prometheus.NewDesc(prometheus.BuildFQName(namespace, "drive", "volume_info"),
			"Current volume of the drive", []string{"library", "drive", "volume"}, nil),
		target: target,
		logger: logger,
	}
}

func (c *DrivesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.online
	ch <- c.state
	ch <- c.volume
}

func (c *DrivesCollector) Collect(ch chan<- prometheus.Metric) {
	level.Debug(c.logger).Log("msg", "Collecting metrics")
	collectTime := time.Now()
	errorMetric := 0
	metrics, err := c.collect()
	if err != nil {
		level.Error(c.logger).Log("msg", err)
		errorMetric = 1
	}

	for _, m := range metrics {
		ch <- prometheus.MustNewConstMetric(c.online, prometheus.GaugeValue, boolToFloat64(m.online), m.library, m.name)
		ch <- prometheus.MustNewConstMetric(c.state, prometheus.GaugeValue, 1, m.library, m.name, m.state)
		ch <- prometheus.MustNewConstMetric(c.volume, prometheus.GaugeValue, 1, m.library, m.name, m.volume)
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "drives")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "drives")
}

func (c *DrivesCollector) collect() ([]DriveMetric, error) {
	out, err := DsmadmcDrivesExec(c.target, c.logger)
	if err != nil {
		return nil, err
	}
	metrics := drivesParse(out, c.logger)
	return metrics, nil
}

func dsmadmcDrives(target *config.Target, logger log.Logger) (string, error) {
	query := "SELECT library_name,drive_name,online,drive_state,volume_name FROM drives"
	if target.LibraryName != "" {
		query = query + fmt.Sprintf(" WHERE library_name='%s'", target.LibraryName)
	}
	out, err := dsmadmcQuery(target, query, *drivesTimeout, logger)
	return out, err
}

func drivesParse(out string, logger log.Logger) []DriveMetric {
	var metrics []DriveMetric
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		l := strings.TrimSpace(line)
		items := strings.Split(l, ",")
		if len(items) != 5 {
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
		metric.state = strings.ToLower(items[3])
		metric.volume = items[4]
		metrics = append(metrics, metric)
	}
	return metrics
}
