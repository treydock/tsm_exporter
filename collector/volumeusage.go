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
	"regexp"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treydock/tsm_exporter/config"
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
		for volumename, count := range m.volumecounts {
			ch <- prometheus.MustNewConstMetric(c.usage, prometheus.GaugeValue, count, m.nodename, volumename)
		}
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "volumeusage")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "volumeusage")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "volumeusage")
}

func (c *VolumeUsagesCollector) collect() ([]VolumeUsageMetric, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*volumeusageTimeout)*time.Second)
	defer cancel()
	out, err := DsmadmcVolumeUsagesExec(c.target, ctx, c.logger)
	if err != nil {
		return nil, err
	}
	metrics, err := volumeusageParse(out, c.target, c.logger)
	return metrics, err
}

func dsmadmcVolumeUsages(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	query := "SELECT DISTINCT VOLUME_NAME,NODE_NAME FROM volumeusage"
	out, err := dsmadmcQuery(target, query, ctx, logger)
	return out, err
}

func volumeusageParse(out string, target *config.Target, logger log.Logger) ([]VolumeUsageMetric, error) {
	nodeVolumes := make(map[string][]string)
	var metrics []VolumeUsageMetric
	records, err := getRecords(out, logger)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if len(record) != 2 {
			continue
		}
		nodeVolumes[record[1]] = append(nodeVolumes[record[1]], record[0])
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
	return metrics, nil
}
