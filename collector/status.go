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
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treydock/tsm_exporter/config"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	statusTimeout     = kingpin.Flag("collector.status.timeout", "Timeout for collecting status information").Default("5").Int()
	DsmadmcStatusExec = dsmadmcStatus
)

type StatusMetric struct {
	serverName string
	reason     string
	status     float64
}

type StatusCollector struct {
	status *prometheus.Desc
	target *config.Target
	logger log.Logger
}

func init() {
	registerCollector("status", true, NewStatusExporter)
}

func NewStatusExporter(target *config.Target, logger log.Logger) Collector {
	return &StatusCollector{
		status: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "status"),
			"Status of TSM, 1=online 0=failure", nil, nil),
		target: target,
		logger: logger,
	}
}

func (c *StatusCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.status
}

func (c *StatusCollector) Collect(ch chan<- prometheus.Metric) {
	level.Debug(c.logger).Log("msg", "Collecting metrics")
	collectTime := time.Now()
	metrics, err := c.collect()
	if err == context.DeadlineExceeded {
		metrics.status = 0
		metrics.reason = "timeout"
	} else if err != nil {
		level.Error(c.logger).Log("msg", err)
		metrics.status = 0
		metrics.reason = "error"
	}

	if metrics.status == 0 {
		level.Error(c.logger).Log("msg", "TSM query status is not healthy",
			"servername", metrics.serverName, "reason", metrics.reason, "err", err)
	}

	ch <- prometheus.MustNewConstMetric(c.status, prometheus.GaugeValue, metrics.status)

	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "status")
}

func (c *StatusCollector) collect() (StatusMetric, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*statusTimeout)*time.Second)
	defer cancel()
	out, err := DsmadmcStatusExec(c.target, ctx, c.logger)
	if err != nil {
		return StatusMetric{}, err
	}
	metrics := statusParse(out, c.logger)
	return metrics, nil
}

func dsmadmcStatus(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	query := "QUERY STATUS"
	out, err := dsmadmcQuery(target, query, ctx, logger)
	return out, err
}

func statusParse(out string, logger log.Logger) StatusMetric {
	var metric StatusMetric
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		l := strings.TrimSpace(line)
		items := strings.Split(l, ",")
		if len(items) < 2 {
			continue
		}
		metric.serverName = items[0]
		metric.status = 1
	}
	if metric.serverName == "" {
		metric.status = 0
		metric.reason = "servername not found"
	}
	return metric
}
