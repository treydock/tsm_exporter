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
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treydock/tsm_exporter/config"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	namespace = "tsm"
)

var (
	exporterUseCache = kingpin.Flag("exporter.use-cache", "Use cached metrics if commands timeout or produce errors").Default("false").Bool()
	dsmLogDir        = kingpin.Flag("path.dsm_log.dir", "Directory to use for DSM_LOG environment variable").Default("/tmp").String()
	execCommand      = exec.CommandContext
	collectorState   = make(map[string]bool)
	factories        = make(map[string]func(target *config.Target, logger log.Logger, useCache bool) Collector)
	collectDuration  = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "exporter", "collector_duration_seconds"),
		"Collector time duration.",
		[]string{"collector"}, nil)
	collectError = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "exporter", "collect_error"),
		"Indicates if error has occurred during collection",
		[]string{"collector"}, nil)
	collecTimeout = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "exporter", "collect_timeout"),
		"Indicates the collector timed out",
		[]string{"collector"}, nil)
)

type Collector interface {
	// Get new metrics and expose them via prometheus registry.
	Describe(ch chan<- *prometheus.Desc)
	Collect(ch chan<- prometheus.Metric)
}

type TSMCollector struct {
	Collectors map[string]Collector
}

func registerCollector(collector string, isDefaultEnabled bool, factory func(target *config.Target, logger log.Logger, useCache bool) Collector) {
	collectorState[collector] = isDefaultEnabled
	factories[collector] = factory
}

func NewCollector(target *config.Target, logger log.Logger) *TSMCollector {
	collectors := make(map[string]Collector)
	for key, enabled := range collectorState {
		enable := false
		if target.Collectors == nil && enabled {
			enable = true
		} else if sliceContains(target.Collectors, key) {
			enable = true
		}
		var collector Collector
		if enable {
			collector = factories[key](target, log.With(logger, "collector", key), *exporterUseCache)
			collectors[key] = collector
		}
	}
	return &TSMCollector{Collectors: collectors}
}

func sliceContains(slice []string, str string) bool {
	for _, s := range slice {
		if str == s {
			return true
		}
	}
	return false
}

func dsmadmcQuery(target *config.Target, query string, ctx context.Context, logger log.Logger) (string, error) {
	servername := fmt.Sprintf("-SERVERName=%s", target.Servername)
	id := fmt.Sprintf("-ID=%s", target.Id)
	password := fmt.Sprintf("-PAssword=%s", target.Password)
	level.Debug(logger).Log("msg", fmt.Sprintf("query=%s", query))
	cmd := execCommand(ctx, "dsmadmc", servername, id, password, "-DATAONLY=YES", "-COMMAdelimited", query)
	os.Setenv("DSM_LOG", *dsmLogDir)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		if strings.Contains(stdout.String(), "No match found using this criteria") {
			return "", nil
		}
		level.Error(logger).Log("msg", "Error executing dsmadc", "err", stderr.String(), "out", stdout.String())
		return "", err
	}
	return stdout.String(), nil
}
