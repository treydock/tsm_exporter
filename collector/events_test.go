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
FOO,Completed,2020-03-22 05:09:43.000000,2020-03-22 05:41:14.000000
FOO,Future,,
BAR,Not Started,,
BAR,Not Started,,
`
)

func TestEventsParse(t *testing.T) {
	metrics := eventsParse(mockEventStdout, &config.Target{Name: "test"}, false, log.NewNopLogger())
	if len(metrics) != 2 {
		t.Errorf("Expected 2 metrics, got %d", len(metrics))
		return
	}
	if val := metrics["FOO"].notCompleted; val != 0 {
		t.Errorf("Expected 0 notCompleted, got %v", val)
	}
	if val := metrics["FOO"].duration; val != 1891 {
		t.Errorf("Expected 1891 duration, got %v", val)
	}
	if val := metrics["BAR"].notCompleted; val != 2 {
		t.Errorf("Expected 1 notCompleted, got %v", val)
	}
}

func TestEventsParseWithSchedules(t *testing.T) {
	metrics := eventsParse(mockEventStdout, &config.Target{Name: "test", Schedules: []string{"BAR"}}, false, log.NewNopLogger())
	if len(metrics) != 1 {
		t.Errorf("Expected 1 metrics, got %d", len(metrics))
	}
	if val := metrics["BAR"].notCompleted; val != 2 {
		t.Errorf("Expected 2 notCompleted, got %v", val)
	}
}

func TestEventsParseDurationCache(t *testing.T) {
	metrics := eventsParse(mockEventStdout, &config.Target{Name: "test"}, true, log.NewNopLogger())
	if len(metrics) != 2 {
		t.Errorf("Expected 2 metrics, got %d", len(metrics))
		return
	}
	if val := metrics["FOO"].notCompleted; val != 0 {
		t.Errorf("Expected 0 notCompleted, got %v", val)
	}
	if val := metrics["FOO"].duration; val != 1891 {
		t.Errorf("Expected 1891 duration, got %v", val)
	}
	stdout := `
FOO,Not Started,,
BAR,Completed,2020-03-22 05:09:44.000000,2020-03-22 05:41:14.000000
`
	metrics = eventsParse(stdout, &config.Target{Name: "test"}, true, log.NewNopLogger())
	if len(metrics) != 2 {
		t.Errorf("Expected 2 metrics, got %d", len(metrics))
		return
	}
	if val := metrics["FOO"].notCompleted; val != 1 {
		t.Errorf("Expected 1 notCompleted, got %v", val)
	}
	if val := metrics["FOO"].duration; val != 1891 {
		t.Errorf("Expected 1891 duration, got %v", val)
	}
	if val := metrics["BAR"].duration; val != 1890 {
		t.Errorf("Expected 1890 duration, got %v", val)
	}
}

func TestEventsCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	cache := false
	useEventDurationCache = &cache
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
	# HELP tsm_schedule_duration_seconds Amount of time taken to complete scheduled event for today, in seconds
	# TYPE tsm_schedule_duration_seconds gauge
	tsm_schedule_duration_seconds{schedule="BAR"} 0
	tsm_schedule_duration_seconds{schedule="FOO"} 1891
    # HELP tsm_schedule_not_completed Number of scheduled events not completed for today
    # TYPE tsm_schedule_not_completed gauge
    tsm_schedule_not_completed{schedule="BAR"} 2
    tsm_schedule_not_completed{schedule="FOO"} 0
	`
	collector := NewEventsExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 7 {
		t.Errorf("Unexpected collection count %d, expected 7", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_schedule_not_completed", "tsm_schedule_duration_seconds",
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
	collector := NewEventsExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_schedule_not_completed", "tsm_schedule_duration_seconds",
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
	collector := NewEventsExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_schedule_not_completed", "tsm_schedule_duration_seconds",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
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
