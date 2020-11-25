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
	"reflect"
	"sort"
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
	dbTimeout     = kingpin.Flag("collector.db.timeout", "Timeout for collecting db information").Default("10").Int()
	DsmadmcDBExec = dsmadmcDB
	dbMap         = map[string]string{
		"DATABASE_NAME":      "Name",
		"TOT_FILE_SYSTEM_MB": "TotalSpace",
		"USED_DB_SPACE_MB":   "UsedSpace",
		"FREE_SPACE_MB":      "FreeSpace",
		"TOTAL_PAGES":        "TotalPages",
		"USABLE_PAGES":       "UsablePages",
		"USED_PAGES":         "UsedPages",
		"FREE_PAGES":         "FreePages",
		"BUFF_HIT_RATIO":     "BuffHitRatio",
		"TOTAL_BUFF_REQ":     "TotalBuffReq",
		"SORT_OVERFLOW":      "SortOverflow",
		"PKG_HIT_RATIO":      "PkgHitRatio",
		"LAST_BACKUP_DATE":   "LastBackup",
	}
)

type DBMetric struct {
	Name         string
	TotalSpace   float64
	UsedSpace    float64
	FreeSpace    float64
	TotalPages   float64
	UsablePages  float64
	UsedPages    float64
	FreePages    float64
	BuffHitRatio float64
	TotalBuffReq float64
	SortOverflow float64
	PkgHitRatio  float64
	LastBackup   string
}

type DBCollector struct {
	TotalSpace   *prometheus.Desc
	UsedSpace    *prometheus.Desc
	FreeSpace    *prometheus.Desc
	TotalPages   *prometheus.Desc
	UsablePages  *prometheus.Desc
	UsedPages    *prometheus.Desc
	FreePages    *prometheus.Desc
	BuffHitRatio *prometheus.Desc
	TotalBuffReq *prometheus.Desc
	SortOverflow *prometheus.Desc
	PkgHitRatio  *prometheus.Desc
	LastBackup   *prometheus.Desc
	target       *config.Target
	logger       log.Logger
}

func init() {
	registerCollector("db", true, NewDBExporter)
}

func NewDBExporter(target *config.Target, logger log.Logger) Collector {
	return &DBCollector{
		TotalSpace: prometheus.NewDesc(prometheus.BuildFQName(namespace, "db", "space_total_bytes"),
			"DB total space in bytes", []string{"dbname"}, nil),
		UsedSpace: prometheus.NewDesc(prometheus.BuildFQName(namespace, "db", "space_used_bytes"),
			"DB used space in bytes", []string{"dbname"}, nil),
		FreeSpace: prometheus.NewDesc(prometheus.BuildFQName(namespace, "db", "space_free_bytes"),
			"DB free space in bytes", []string{"dbname"}, nil),
		TotalPages: prometheus.NewDesc(prometheus.BuildFQName(namespace, "db", "pages_total"),
			"DB total pages", []string{"dbname"}, nil),
		UsablePages: prometheus.NewDesc(prometheus.BuildFQName(namespace, "db", "pages_usable"),
			"DB usable pages", []string{"dbname"}, nil),
		UsedPages: prometheus.NewDesc(prometheus.BuildFQName(namespace, "db", "pages_used"),
			"DB used pages", []string{"dbname"}, nil),
		FreePages: prometheus.NewDesc(prometheus.BuildFQName(namespace, "db", "pages_free"),
			"DB free pages", []string{"dbname"}, nil),
		BuffHitRatio: prometheus.NewDesc(prometheus.BuildFQName(namespace, "db", "buffer_hit_ratio"),
			"DB buffer hit ratio (0.0-1.0)", []string{"dbname"}, nil),
		TotalBuffReq: prometheus.NewDesc(prometheus.BuildFQName(namespace, "db", "buffer_requests_total"),
			"DB total buffer requests", []string{"dbname"}, nil),
		SortOverflow: prometheus.NewDesc(prometheus.BuildFQName(namespace, "db", "sort_overflow"),
			"DB sort overflow", []string{"dbname"}, nil),
		PkgHitRatio: prometheus.NewDesc(prometheus.BuildFQName(namespace, "db", "pkg_hit_ratio"),
			"DB pkg hit ratio (0.0-1.0)", []string{"dbname"}, nil),
		LastBackup: prometheus.NewDesc(prometheus.BuildFQName(namespace, "db", "last_backup_timestamp_seconds"),
			"Time since last backup in epoch", []string{"dbname"}, nil),
		target: target,
		logger: logger,
	}
}

func (c *DBCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.TotalSpace
	ch <- c.UsedSpace
	ch <- c.FreeSpace
	ch <- c.TotalPages
	ch <- c.UsablePages
	ch <- c.UsedPages
	ch <- c.FreePages
	ch <- c.BuffHitRatio
	ch <- c.TotalBuffReq
	ch <- c.SortOverflow
	ch <- c.PkgHitRatio
	ch <- c.LastBackup
}

func (c *DBCollector) Collect(ch chan<- prometheus.Metric) {
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
		ch <- prometheus.MustNewConstMetric(c.TotalSpace, prometheus.GaugeValue, m.TotalSpace, m.Name)
		ch <- prometheus.MustNewConstMetric(c.UsedSpace, prometheus.GaugeValue, m.UsedSpace, m.Name)
		ch <- prometheus.MustNewConstMetric(c.FreeSpace, prometheus.GaugeValue, m.FreeSpace, m.Name)
		ch <- prometheus.MustNewConstMetric(c.TotalPages, prometheus.GaugeValue, m.TotalPages, m.Name)
		ch <- prometheus.MustNewConstMetric(c.UsablePages, prometheus.GaugeValue, m.UsablePages, m.Name)
		ch <- prometheus.MustNewConstMetric(c.UsedPages, prometheus.GaugeValue, m.UsedPages, m.Name)
		ch <- prometheus.MustNewConstMetric(c.FreePages, prometheus.GaugeValue, m.FreePages, m.Name)
		ch <- prometheus.MustNewConstMetric(c.BuffHitRatio, prometheus.GaugeValue, m.BuffHitRatio, m.Name)
		ch <- prometheus.MustNewConstMetric(c.TotalBuffReq, prometheus.CounterValue, m.TotalBuffReq, m.Name)
		ch <- prometheus.MustNewConstMetric(c.SortOverflow, prometheus.GaugeValue, m.SortOverflow, m.Name)
		ch <- prometheus.MustNewConstMetric(c.PkgHitRatio, prometheus.GaugeValue, m.PkgHitRatio, m.Name)
		lastBackup, err := time.Parse("2006-01-02 15:04:05.000000", m.LastBackup)
		if err == nil {
			ch <- prometheus.MustNewConstMetric(c.LastBackup, prometheus.GaugeValue, float64(lastBackup.Unix()), m.Name)
		} else {
			level.Error(c.logger).Log("msg", fmt.Sprintf("Error parsing lastBackup value %s: %s", m.LastBackup, err.Error()))
		}
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "db")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "db")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "db")
}

func (c *DBCollector) collect() ([]DBMetric, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*dbTimeout)*time.Second)
	defer cancel()
	out, err := DsmadmcDBExec(c.target, ctx, c.logger)
	if err != nil {
		return nil, err
	}
	metrics := dbParse(out, c.logger)
	return metrics, nil
}

func dsmadmcDB(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
	fields := getDBFields()
	query := fmt.Sprintf("SELECT %s FROM db", strings.Join(fields, ","))
	out, err := dsmadmcQuery(target, query, ctx, logger)
	return out, err
}

func dbParse(out string, logger log.Logger) []DBMetric {
	var metrics []DBMetric
	fields := getDBFields()
	lines := strings.Split(out, "\n")
	for _, l := range lines {
		values := strings.Split(strings.TrimSpace(l), ",")
		if len(values) != len(fields) {
			continue
		}
		var metric DBMetric
		ps := reflect.ValueOf(&metric) // pointer to struct - addressable
		s := ps.Elem()                 //struct
		for i, k := range fields {
			field := dbMap[k]
			f := s.FieldByName(field)
			if f.Kind() == reflect.String {
				f.SetString(values[i])
			} else {
				val, err := strconv.ParseFloat(values[i], 64)
				if err != nil {
					level.Error(logger).Log("msg", fmt.Sprintf("Error parsing %s value %s: %s", k, values[i], err.Error()))
					continue
				}
				if strings.HasSuffix(k, "_MB") {
					val = val * 1024 * 1024
				} else if strings.HasSuffix(k, "_RATIO") {
					val = val / 100
				}
				f.SetFloat(val)
			}
		}
		metrics = append(metrics, metric)
	}
	return metrics
}

func getDBFields() []string {
	var fields []string
	for k := range dbMap {
		fields = append(fields, k)
	}
	sort.Strings(fields)
	return fields
}
