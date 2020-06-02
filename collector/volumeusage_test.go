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
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/treydock/tsm_exporter/config"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	mockVolumeUsageStdout = `
NETAPPUSER2,F00665L7
NETAPPUSER2,F00665L7
NETAPPUSER2,F00665L7
NETAPPUSER2,E00665L7
ESS2_ENC,E00168L6
ESS2_ENC,E00170L6
ESS2_ENC,/fs/diskpool/sp02/ess/vol51
`
)

func TestVolumeUsagesParse(t *testing.T) {
	target := &config.Target{VolumeUsageMap: map[string]string{
		"LTO6": "^E",
		"LTO7": "^F",
	}}
	metrics := volumeusageParse(mockVolumeUsageStdout, target, log.NewNopLogger())
	if len(metrics) != 2 {
		t.Errorf("Expected 2 metrics, got %v", len(metrics))
	}
	var metric VolumeUsageMetric
	for _, m := range metrics {
		if m.nodename == "NETAPPUSER2" {
			metric = m
			break
		}
	}
	if metric.nodename == "" {
		t.Fatal("Did not find NETAPPUSER2 metric")
	}
	if val := len(metric.volumecounts); val != 2 {
		t.Errorf("Expected 2 volume counts, got %d", val)
	}
}

func TestVolumeUsagesParseNoMap(t *testing.T) {
	metrics := volumeusageParse(mockVolumeUsageStdout, &config.Target{}, log.NewNopLogger())
	if len(metrics) != 2 {
		t.Errorf("Expected 2 metrics, got %v", len(metrics))
	}
	if val := len(metrics[0].volumecounts); val != 1 {
		t.Errorf("Expected 1 volume counts, got %d", val)
	}
}

func TestVolumeUsagesCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcVolumeUsagesExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockVolumeUsageStdout, nil
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="volumeusage"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="volumeusage"} 0
	# HELP tsm_volume_usage Number of volumes used by node name
	# TYPE tsm_volume_usage gauge
	tsm_volume_usage{nodename="ESS2_ENC",volumename="LTO6"} 2
	tsm_volume_usage{nodename="NETAPPUSER2",volumename="LTO6"} 1
	tsm_volume_usage{nodename="NETAPPUSER2",volumename="LTO7"} 3
	`
	target := &config.Target{VolumeUsageMap: map[string]string{
		"LTO6": "^E",
		"LTO7": "^F",
	}}
	collector := NewVolumeUsagesExporter(target, log.NewNopLogger(), false)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 6 {
		t.Errorf("Unexpected collection count %d, expected 6", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_volume_usage",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestVolumeUsagesCollectorError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcVolumeUsagesExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="volumeusage"} 1
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="volumeusage"} 0
	`
	collector := NewVolumeUsagesExporter(&config.Target{}, log.NewNopLogger(), false)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_volume_usage",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestVolumeUsagesCollectorTimeout(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcVolumeUsagesExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", context.DeadlineExceeded
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="volumeusage"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="volumeusage"} 1
	`
	collector := NewVolumeUsagesExporter(&config.Target{}, log.NewNopLogger(), false)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_volume_usage",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestVolumeUsagesCollectorCache(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcVolumeUsagesExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockVolumeUsageStdout, nil
	}
	expected := `
	# HELP tsm_volume_usage Number of volumes used by node name
	# TYPE tsm_volume_usage gauge
	tsm_volume_usage{nodename="ESS2_ENC",volumename="LTO6"} 2
	tsm_volume_usage{nodename="NETAPPUSER2",volumename="LTO6"} 1
	tsm_volume_usage{nodename="NETAPPUSER2",volumename="LTO7"} 3
	`
	errorMetric := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="volumeusage"} 1
	`
	timeoutMetric := `
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="volumeusage"} 1
	`
	target := &config.Target{VolumeUsageMap: map[string]string{
		"LTO6": "^E",
		"LTO7": "^F",
	}}
	collector := NewVolumeUsagesExporter(target, log.NewNopLogger(), true)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 6 {
		t.Errorf("Unexpected collection count %d, expected 6", val)
	}
	DsmadmcVolumeUsagesExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	if val := testutil.CollectAndCount(collector); val != 6 {
		t.Errorf("Unexpected collection count %d, expected 6", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(errorMetric+expected),
		"tsm_volume_usage", "tsm_exporter_collect_error"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
	DsmadmcVolumeUsagesExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", context.DeadlineExceeded
	}
	if val := testutil.CollectAndCount(collector); val != 6 {
		t.Errorf("Unexpected collection count %d, expected 6", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(timeoutMetric+expected),
		"tsm_volume_usage", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestDsmadmcVolumeUsages(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 0
	mockedStdout = "foo"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dsmadmcVolumeUsages(&config.Target{}, ctx, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != mockedStdout {
		t.Errorf("Unexpected out: %s", out)
	}
}
