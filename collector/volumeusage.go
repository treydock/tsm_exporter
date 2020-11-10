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
	"regexp"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treydock/tsm_exporter/config"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	volumeusageTimeout      = kingpin.Flag("collector.volumeusage.timeout", "Timeout for collecting volumeusage information").Default("5").Int()
	DsmadmcVolumeUsagesExec = dsmadmcVolumeUsages
)

type VolumeUsageMetric struct {
	nodename     string
	volumecounts map[string]float64
}

type VolumeUsagesCollector struct {
	usage  *prometheus.Desc
	target *config.Target
	logger log.Logger
}

func init() {
	registerCollector("volumeusage", true, NewVolumeUsagesExporter)
}

func NewVolumeUsagesExporter(target *config.Target, logger log.Logger) Collector {
	return &VolumeUsagesCollector{
		usage: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volume", "usage"),
			"Number of volumes used by node name", []string{"nodename", "volumename"}, nil),
		target: target,
		logger: logger,
	}
}

func (c *VolumeUsagesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.usage
}

func (c *VolumeUsagesCollector) Collect(ch chan<- prometheus.Metric) {
	level.Debug(c.logger).Log("msg", "Collecting metrics")
	collectTime := time.Now()
	errorMetric := 0
	metrics, err := c.collect()
	if err != nil {
		level.Error(c.logger).Log("msg", err)
		errorMetric = 1
	}

	for _, m := range metrics {
		for volumename, count := range m.volumecounts {
			ch <- prometheus.MustNewConstMetric(c.usage, prometheus.GaugeValue, count, m.nodename, volumename)
		}
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "volumeusage")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "volumeusage")
}

func (c *VolumeUsagesCollector) collect() ([]VolumeUsageMetric, error) {
	out, err := DsmadmcVolumeUsagesExec(c.target, c.logger)
	if err != nil {
		return nil, err
	}
	metrics := volumeusageParse(out, c.target, c.logger)
	return metrics, nil
}

func dsmadmcVolumeUsages(target *config.Target, logger log.Logger) (string, error) {
	query := "SELECT NODE_NAME,VOLUME_NAME FROM volumeusage"
	out, err := dsmadmcQuery(target, query, *volumeusageTimeout, logger)
	return out, err
}

func volumeusageParse(out string, target *config.Target, logger log.Logger) []VolumeUsageMetric {
	nodeVolumes := make(map[string][]string)
	var metrics []VolumeUsageMetric
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		l := strings.TrimSpace(line)
		items := strings.Split(l, ",")
		if len(items) != 2 {
			continue
		}
		nodeVolumes[items[0]] = append(nodeVolumes[items[0]], items[1])
	}
	for nodename, volumes := range nodeVolumes {
		var metric VolumeUsageMetric
		volumecounts := make(map[string]float64)
		metric.nodename = nodename
		for _, volume := range volumes {
			var volumename string
			if target.VolumeUsageMap == nil {
				volumename = "all"
			} else {
				for name, regex := range target.VolumeUsageMap {
					pattern := regexp.MustCompile(regex)
					if pattern.MatchString(volume) {
						volumename = name
						break
					}
				}
			}
			if volumename == "" {
				continue
			}
			volumecounts[volumename]++
		}
		metric.volumecounts = volumecounts
		metrics = append(metrics, metric)
	}
	return metrics
}
