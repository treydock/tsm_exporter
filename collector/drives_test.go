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
	mockDriveStdout = `
LIB1,TAPE10,YES,LOADED,FOO1
LIB1,TAPE11,YES,LOADED,FOO2
LIBENC,TAPE00,YES,EMPTY,
LIBENC,TAPE01,NO,EMPTY,
`
)

func TestDrivesParse(t *testing.T) {
	metrics := drivesParse(mockDriveStdout, log.NewNopLogger())
	if len(metrics) != 4 {
		t.Errorf("Expected 4 metrics, got %d", len(metrics))
	}
	if metrics[0].online != true {
		t.Errorf("Expected online, got %v", metrics[0].online)
	}
}

func TestDrivesCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcDrivesExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockDriveStdout, nil
	}
	expected := `
	# HELP tsm_drive_online Inidicates if the drive is online, 1=online, 0=offline
	# TYPE tsm_drive_online gauge
	tsm_drive_online{drive="TAPE10",library="LIB1"} 1
	tsm_drive_online{drive="TAPE11",library="LIB1"} 1
	tsm_drive_online{drive="TAPE00",library="LIBENC"} 1
	tsm_drive_online{drive="TAPE01",library="LIBENC"} 0
	# HELP tsm_drive_state_info Current state of the drive
	# TYPE tsm_drive_state_info gauge
	tsm_drive_state_info{drive="TAPE10",library="LIB1",state="loaded"} 1
	tsm_drive_state_info{drive="TAPE11",library="LIB1",state="loaded"} 1
	tsm_drive_state_info{drive="TAPE00",library="LIBENC",state="empty"} 1
	tsm_drive_state_info{drive="TAPE01",library="LIBENC",state="empty"} 1
	# HELP tsm_drive_volume_info Current volume of the drive
	# TYPE tsm_drive_volume_info gauge
	tsm_drive_volume_info{drive="TAPE10",library="LIB1",volume="FOO1"} 1
	tsm_drive_volume_info{drive="TAPE11",library="LIB1",volume="FOO2"} 1
	tsm_drive_volume_info{drive="TAPE00",library="LIBENC",volume=""} 1
	tsm_drive_volume_info{drive="TAPE01",library="LIBENC",volume=""} 1
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="drives"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="drives"} 0
	`
	collector := NewDrivesExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 15 {
		t.Errorf("Unexpected collection count %d, expected 15", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_drive_online", "tsm_drive_state_info", "tsm_drive_volume_info",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestDrivesCollectorError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcDrivesExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="drives"} 1
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="drives"} 0
	`
	collector := NewDrivesExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_drive_online", "tsm_drive_state_info", "tsm_drive_volume_info",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestDrivesCollectorTimeout(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcDrivesExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", context.DeadlineExceeded
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="drives"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="drives"} 1
	`
	collector := NewDrivesExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_drive_online", "tsm_drive_state_info", "tsm_drive_volume_info",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestDsmadmcDrives(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 0
	mockedStdout = "foo"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dsmadmcDrives(&config.Target{}, ctx, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != mockedStdout {
		t.Errorf("Unexpected out: %s", out)
	}
}
