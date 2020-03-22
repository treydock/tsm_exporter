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
	mockEventStdout = `
FOO,Completed
FOO,Completed
BAR,Not Started
BAR,Not Started
`
)

func TestEventsParse(t *testing.T) {
	metrics, err := eventsParse(mockEventStdout, nil, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected err: %s", err.Error())
		return
	}
	if len(metrics) != 2 {
		t.Errorf("Expected 2 metrics, got %d", len(metrics))
		return
	}
	if val := metrics[0].notstarted; val != 0 {
		t.Errorf("Expected 0 notstarted, got %v", val)
	}
	if val := metrics[1].notstarted; val != 2 {
		t.Errorf("Expected 2 notstarted, got %v", val)
	}
}

func TestEventsParseWithSchedules(t *testing.T) {
	metrics, err := eventsParse(mockEventStdout, []string{"BAR"}, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected err: %s", err.Error())
		return
	}
	if len(metrics) != 1 {
		t.Errorf("Expected 3 metrics, got %d", len(metrics))
	}
	if val := metrics[0].notstarted; val != 2 {
		t.Errorf("Expected 2 notstarted, got %v", val)
	}
}

func TestEventsCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcEventsExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockEventStdout, nil
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="events"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="events"} 0
    # HELP tsm_schedule_notstarted Number of schedules not started for today
    # TYPE tsm_schedule_notstarted gauge
    tsm_schedule_notstarted{schedule="BAR"} 2
    tsm_schedule_notstarted{schedule="FOO"} 0
	`
	collector := NewEventsExporter(&config.Target{}, log.NewNopLogger(), false)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 5 {
		t.Errorf("Unexpected collection count %d, expected 5", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_schedule_notstarted",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestEventsCollectorError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcEventsExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="events"} 1
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="events"} 0
	`
	collector := NewEventsExporter(&config.Target{}, log.NewNopLogger(), false)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_schedule_notstarted",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestEventsCollectorTimeout(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcEventsExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", context.DeadlineExceeded
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="events"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="events"} 1
	`
	collector := NewEventsExporter(&config.Target{}, log.NewNopLogger(), false)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_schedule_notstarted",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestEventsCollectorCache(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcEventsExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockEventStdout, nil
	}
	expected := `
    # HELP tsm_schedule_notstarted Number of schedules not started for today
    # TYPE tsm_schedule_notstarted gauge
    tsm_schedule_notstarted{schedule="BAR"} 2
    tsm_schedule_notstarted{schedule="FOO"} 0
	`
	errorMetric := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="events"} 1
	`
	timeoutMetric := `
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="events"} 1
	`
	collector := NewEventsExporter(&config.Target{}, log.NewNopLogger(), true)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 5 {
		t.Errorf("Unexpected collection count %d, expected 5", val)
	}
	DsmadmcEventsExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	if val := testutil.CollectAndCount(collector); val != 5 {
		t.Errorf("Unexpected collection count %d, expected 5", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(errorMetric+expected),
		"tsm_schedule_notstarted", "tsm_exporter_collect_error"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
	DsmadmcEventsExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", context.DeadlineExceeded
	}
	if val := testutil.CollectAndCount(collector); val != 5 {
		t.Errorf("Unexpected collection count %d, expected 5", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(timeoutMetric+expected),
		"tsm_schedule_notstarted", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestDsmadmcEvents(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 0
	mockedStdout = "foo"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dsmadmcEvents(&config.Target{}, ctx, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != mockedStdout {
		t.Errorf("Unexpected out: %s", out)
	}
}
