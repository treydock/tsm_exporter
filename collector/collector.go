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
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/google/goexpect"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treydock/tsm_exporter/config"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	namespace  = "tsm"
	timeFormat = "2006-01-02 15:04:05.000000"
)

var (
	dsmLogDir             = kingpin.Flag("path.dsm_log.dir", "Directory to use for DSM_LOG environment variable").Default("/tmp").String()
	dsmadmcTimeout        = kingpin.Flag("collector.dsmadmc.timeout", "Timeout of executing dsmadmc").Default("5").Int()
	dsmadmcDebug          = kingpin.Flag("collector.dsmadmc.debug", "Debug executing of dsmadmc commands").Default("false").Bool()
	dsmadmcPasswordPrompt = kingpin.Flag("collector.dsmadmc.password-prompt", "Regexp of password prompt").Default("password:").String()
	dsmadmcPrompt         = kingpin.Flag("collector.dsmadmc.prompt", "Regexp of prompt for dsmadmc").Default("(?m)Protect:.+>").String()
	spawnExpectFunc       = spawnExpect
	timeNow               = time.Now
	queryOutput           = getQueryOutput
	collectorState        = make(map[string]bool)
	factories             = make(map[string]func(target *config.Target, logger log.Logger) Collector)
	collectDuration       = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "exporter", "collector_duration_seconds"),
		"Collector time duration.",
		[]string{"collector"}, nil)
	collectError = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "exporter", "collect_error"),
		"Indicates if error has occurred during collection",
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

func registerCollector(collector string, isDefaultEnabled bool, factory func(target *config.Target, logger log.Logger) Collector) {
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
			collector = factories[key](target, log.With(logger, "collector", key, "target", target.Name))
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

func boolToFloat64(data bool) float64 {
	if data {
		return float64(1)
	} else {
		return float64(0)
	}
}

func spawnExpect(cmd string, timeout time.Duration) (*expect.GExpect, error) {
	opts := []expect.Option{
		expect.NoCheck(),
	}
	if *dsmadmcDebug {
		opts = append(opts, expect.Verbose(true))
		opts = append(opts, expect.VerboseWriter(os.Stdout))
	}
	e, _, err := expect.Spawn(cmd, timeout, opts...)
	return e, err
}

func dsmadmcQuery(target *config.Target, query string, queryTimeout int, logger log.Logger) (string, error) {
	level.Debug(logger).Log("msg", "dsmadmc query", "query", query)
	passwordRE := regexp.MustCompile(*dsmadmcPasswordPrompt)
	promptRE := regexp.MustCompile(*dsmadmcPrompt)
	timeout := time.Duration(*dsmadmcTimeout) * time.Second
	tmp, err := ioutil.TempFile("", "tsm_exporter_out")
	defer os.Remove(tmp.Name())
	if err != nil {
		level.Error(logger).Log("msg", "Error creating temporary output file", "err", err)
		return "", err
	}
	cmd := fmt.Sprintf("dsmadmc -DATAONLY=YES -COMMAdelimited -SERVERName=%s -ID=%s -OUTfile=%s -Quiet", target.Servername, target.Id, tmp.Name())
	os.Setenv("DSM_LOG", *dsmLogDir)
	e, err := spawnExpectFunc(cmd, timeout)
	if err != nil {
		level.Error(logger).Log("msg", "Error executing dsmadc", "err", err)
		return "", err
	}
	defer e.Close()

	_, _, err = e.Expect(passwordRE, timeout)
	if err != nil {
		level.Error(logger).Log("msg", "Error gettig password prompt", "err", err)
		return "", err
	}
	err = e.Send(target.Password + "\n")
	if err != nil {
		level.Error(logger).Log("msg", "Error sending password", "err", err)
		return "", err
	}
	_, _, err = e.Expect(promptRE, timeout)
	if err != nil {
		level.Error(logger).Log("msg", "Error gettig prompt before query", "err", err)
		return "", err
	}
	err = e.Send(query + "\n")
	if err != nil {
		level.Error(logger).Log("msg", "Error sending query", "err", err)
		return "", err
	}
	_, _, err = e.Expect(promptRE, time.Duration(queryTimeout)*time.Second)
	if err != nil {
		level.Error(logger).Log("msg", "Error querying dsmadc", "err", err)
		return "", err
	}
	err = e.Send("quit\n")
	if err != nil {
		level.Error(logger).Log("msg", "Error sending quit", "err", err)
		return "", err
	}
	output, err := queryOutput(tmp.Name(), query, logger)
	return output, err
}

func getQueryOutput(path string, query string, logger log.Logger) (string, error) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		level.Error(logger).Log("msg", "Unable to read output file", "file", path, "err", err)
		return "", err
	}
	result := string(content)
	result = strings.Trim(result, query)
	result = strings.Trim(result, "quit\n")
	result = strings.TrimSpace(result)
	if strings.Contains(result, "No match found using this criteria") {
		return "", nil
	}
	level.Debug(logger).Log("msg", "query output", "result", result)
	return result, nil
}

func buildInFilter(items []string) string {
	var values []string
	for _, s := range items {
		values = append(values, fmt.Sprintf("'%s'", s))
	}
	return strings.Join(values, ",")
}
