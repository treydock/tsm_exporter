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

func TestVolumesUnavailParse(t *testing.T) {
	mockStdout := "1\n"
	metrics, err := volumesUnavailParse(mockStdout, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected err: %s", err.Error())
		return
	}
	if metrics.unavailable != 1 {
		t.Errorf("Expected 1 unavailable, got %v", metrics.unavailable)
	}
}

func TestVolumesCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcVolumesUnavailExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "1\n", nil
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="volumes"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="volumes"} 0
    # HELP tsm_volumes_unavailable Number of unavailable volumes
    # TYPE tsm_volumes_unavailable gauge
    tsm_volumes_unavailable 1
	`
	collector := NewVolumesExporter(&config.Target{}, log.NewNopLogger(), false)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 4 {
		t.Errorf("Unexpected collection count %d, expected 4", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_volumes_unavailable", "tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestVolumesCollectorError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcVolumesUnavailExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="volumes"} 1
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="volumes"} 0
	`
	collector := NewVolumesExporter(&config.Target{}, log.NewNopLogger(), false)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_volumes_unavailable", "tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestVolumesCollectorTimeout(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcVolumesUnavailExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", context.DeadlineExceeded
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="volumes"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="volumes"} 1
	`
	collector := NewVolumesExporter(&config.Target{}, log.NewNopLogger(), false)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_volumes_unavailable", "tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestVolumesCollectorCache(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcVolumesUnavailExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "1\n", nil
	}
	expected := `
    # HELP tsm_volumes_unavailable Number of unavailable volumes
    # TYPE tsm_volumes_unavailable gauge
    tsm_volumes_unavailable 1
	`
	errorMetric := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="volumes"} 1
	`
	timeoutMetric := `
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="volumes"} 1
	`
	collector := NewVolumesExporter(&config.Target{}, log.NewNopLogger(), true)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 4 {
		t.Errorf("Unexpected collection count %d, expected 4", val)
	}
	DsmadmcVolumesUnavailExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	if val := testutil.CollectAndCount(collector); val != 4 {
		t.Errorf("Unexpected collection count %d, expected 4", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(errorMetric+expected),
		"tsm_volumes_unavailable", "tsm_exporter_collect_error"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
	DsmadmcVolumesUnavailExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", context.DeadlineExceeded
	}
	if val := testutil.CollectAndCount(collector); val != 4 {
		t.Errorf("Unexpected collection count %d, expected 4", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(timeoutMetric+expected),
		"tsm_volumes_unavailable", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestDsmadmcVolumesUnavail(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 0
	mockedStdout = "foo"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dsmadmcVolumesUnavail(&config.Target{}, ctx, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != mockedStdout {
		t.Errorf("Unexpected out: %s", out)
	}
}
