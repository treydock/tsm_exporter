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
)

type LibVolumeMetric struct {
	mediatype string
	library   string
	private   float64
	scratch   float64
}

type LibVolumesCollector struct {
	media  *prometheus.Desc
	target *config.Target
	logger log.Logger
}

func init() {
	registerCollector("libvolumes", true, NewLibVolumesExporter)
}

func NewLibVolumesExporter(target *config.Target, logger log.Logger) Collector {
	return &LibVolumesCollector{
		media: prometheus.NewDesc(prometheus.BuildFQName(namespace, "libvolume", "media"),
			"Number of tapes", []string{"mediatype", "library", "status"}, nil),
		target: target,
		logger: logger,
	}
}

func (c *LibVolumesCollector) Describe(ch chan<- *prometheus.Desc) {
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

	for _, m := range metrics {
		ch <- prometheus.MustNewConstMetric(c.media, prometheus.GaugeValue, m.scratch, m.mediatype, m.library, "scratch")
		ch <- prometheus.MustNewConstMetric(c.media, prometheus.GaugeValue, m.private, m.mediatype, m.library, "private")
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "libvolumes")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "libvolumes")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "libvolumes")
}

func (c *LibVolumesCollector) collect() (map[string]LibVolumeMetric, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*libvolumesTimeout)*time.Second)
	defer cancel()
	out, err := DsmadmcLibVolumesExec(c.target, ctx, c.logger)
	if err != nil {
		return nil, err
	}
	metrics, err := libvolumesParse(out, c.logger)
	return metrics, err
}

func buildLibVolumesQuery(target *config.Target) string {
	query := "SELECT MEDIATYPE,STATUS,LIBRARY_NAME,COUNT(*) FROM libvolumes"
	if target.LibraryName != "" {
		query = query + fmt.Sprintf(" WHERE LIBRARY_NAME='%s'", target.LibraryName)
	}
	query = query + " GROUP BY(MEDIATYPE,STATUS,LIBRARY_NAME)"
	return query
}

func dsmadmcLibVolumes(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	out, err := dsmadmcQuery(target, buildLibVolumesQuery(target), ctx, logger)
	return out, err
}

func libvolumesParse(out string, logger log.Logger) (map[string]LibVolumeMetric, error) {
	metrics := make(map[string]LibVolumeMetric)
	records, err := getRecords(out, logger)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if len(record) != 4 {
			continue
		}
		var metric LibVolumeMetric
		mediatype := record[0]
		library := record[2]
		key := fmt.Sprintf("%s-%s", mediatype, library)
		if val, ok := metrics[key]; ok {
			metric = val
		} else {
			metric.mediatype = mediatype
			metric.library = library
		}
		status := strings.ToLower(record[1])
		count, err := parseFloat(record[3])
		if err != nil {
			level.Error(logger).Log("msg", "Error parsing libvolume value", "value", record[3], "record", strings.Join(record, ","), "err", err)
			return nil, err
		}
		switch status {
		case "scratch":
			metric.scratch += count
		case "private":
			metric.private += count
		default:
			level.Error(logger).Log("msg", "Unknown libvolume status encountered", "status", status, "record", strings.Join(record, ","))
			return nil, fmt.Errorf("Unknown libvolume status encountered: %s", status)
		}
		metrics[key] = metric
	}
	return metrics, nil
}
