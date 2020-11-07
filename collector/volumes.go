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
	volumesTimeout          = kingpin.Flag("collector.volumes.timeout", "Timeout for collecting volumes information").Default("10").Int()
	volumesClassnameExclude = kingpin.Flag("collector.volumes.classname-exclude", "Regexp of classname of exclude").Default("").String()
	DsmadmcVolumesExec      = dsmadmcVolumes
)

type VolumeMetric struct {
	name      string
	classname string
	access    string
	utilized  float64
	capacity  float64
}

type VolumesCollector struct {
	unavailable *prometheus.Desc
	readonly    *prometheus.Desc
	utilized    *prometheus.Desc
	capacity    *prometheus.Desc
	target      *config.Target
	logger      log.Logger
}

func init() {
	registerCollector("volumes", true, NewVolumesExporter)
}

func NewVolumesExporter(target *config.Target, logger log.Logger) Collector {
	return &VolumesCollector{
		unavailable: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volumes", "unavailable"),
			"Number of unavailable volumes", nil, nil),
		readonly: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volumes", "readonly"),
			"Number of readonly volumes", nil, nil),
		utilized: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volume", "utilized_percent"),
			"Volume percent utilized", []string{"name", "classname"}, nil),
		capacity: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volume", "estimated_capacity_bytes"),
			"Volume estimated capacity", []string{"name", "classname"}, nil),
		target: target,
		logger: logger,
	}
}

func (c *VolumesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.unavailable
	ch <- c.readonly
	ch <- c.utilized
	ch <- c.capacity
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

	var unavailable float64
	var readonly float64
	for _, m := range metrics {
		switch m.access {
		case "UNAVAILABLE":
			unavailable++
		case "READONLY":
			readonly++
		}
		ch <- prometheus.MustNewConstMetric(c.utilized, prometheus.GaugeValue, m.utilized, m.name, m.classname)
		ch <- prometheus.MustNewConstMetric(c.capacity, prometheus.GaugeValue, m.capacity, m.name, m.classname)
	}
	if err == nil {
		ch <- prometheus.MustNewConstMetric(c.unavailable, prometheus.GaugeValue, unavailable)
		ch <- prometheus.MustNewConstMetric(c.readonly, prometheus.GaugeValue, readonly)
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "volumes")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "volumes")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "volumes")
}

func (c *VolumesCollector) collect() ([]VolumeMetric, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*volumesTimeout)*time.Second)
	defer cancel()
	out, err := DsmadmcVolumesExec(c.target, ctx, c.logger)
	if err != nil {
		return nil, err
	}
	metrics := volumesParse(out, c.logger)
	return metrics, nil
}

func dsmadmcVolumes(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	query := "SELECT access,est_capacity_mb,pct_utilized,devclass_name,volume_name FROM volumes"
	out, err := dsmadmcQuery(target, query, ctx, logger)
	return out, err
}

func volumesParse(out string, logger log.Logger) []VolumeMetric {
	classnameExcludePattern := regexp.MustCompile(*volumesClassnameExclude)
	var metrics []VolumeMetric
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		l := strings.TrimSpace(line)
		items := strings.Split(l, ",")
		if len(items) != 5 {
			continue
		}
		var metric VolumeMetric
		metric.name = items[4]
		metric.classname = items[3]
		if *volumesClassnameExclude != "" && classnameExcludePattern.MatchString(metric.classname) {
			level.Debug(logger).Log("msg", "Skipping volume due to classname exclude", "volume", metric.name, "classname", metric.classname)
			continue
		}
		metric.access = items[0]
		capacity, err := strconv.ParseFloat(items[1], 64)
		if err != nil {
			level.Error(logger).Log("msg", "Error parsing est_capacity_mb", "value", items[1], "err", err)
			continue
		}
		metric.capacity = capacity * 1024 * 1024
		utilized, err := strconv.ParseFloat(items[2], 64)
		if err != nil {
			level.Error(logger).Log("msg", "Error parsing pct_utilized value", "value", items[2], "err", err)
			continue
		}
		metric.utilized = utilized
		metrics = append(metrics, metric)
	}
	return metrics
}
