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
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/treydock/tsm_exporter/config"
)

var (
	mockVolumeStdout = `
READWRITE,512000.0,0.0,DCFILEE,/fs/diskpool/sp02/enc-gpfs/svol49
READWRITE,2396430.0,100.0,DCULT6E,E00090L6
READWRITE,9067296.0,43.7,DCULT7,F00397L7
UNAVAILABLE,8199467.0,68.5,DCULT7,F00640L7
READONLY,5735346.0,94.6,DCULT7,F00529L7
`
	mockVolumeStdoutComma = `
READWRITE,"512000,0","0,0",DCFILEE,/fs/diskpool/sp02/enc-gpfs/svol49
READWRITE,"2396430,0","100,0",DCULT6E,E00090L6
READWRITE,"9067296,0","43,7",DCULT7,F00397L7
UNAVAILABLE,"8199467,0","68,5",DCULT7,F00640L7
READONLY,"5735346,0","94,6",DCULT7,F00529L7
`
)

func TestVolumesParse(t *testing.T) {
	metrics, err := volumesParse(mockVolumeStdout, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		return
	}
	if val := len(metrics); val != 5 {
		t.Errorf("Expected 5 metrics, got %d", val)
	}
	if val := metrics[1].utilized; val != 1.0 {
		t.Errorf("Unexpected utilized, got %v", val)
	}
}

func TestVolumesParseComma(t *testing.T) {
	metrics, err := volumesParse(mockVolumeStdoutComma, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		return
	}
	if val := len(metrics); val != 5 {
		t.Errorf("Expected 5 metrics, got %d", val)
	}
	if val := metrics[1].utilized; val != 1.0 {
		t.Errorf("Unexpected utilized, got %v", val)
	}
}

func TestVolumesParseError(t *testing.T) {
	tests := []string{
		"READWRITE,foo,100.0,DCULT6E,E00090L6\n",
		"READWRITE,2396430.0,foo,DCULT6E,E00090L6\n",
		"UNAVAILABLE,\"8199467\",0\",\"68,5\",DCULT7,F00640L7",
	}
	for i, out := range tests {
		_, err := volumesParse(out, log.NewNopLogger())
		if err == nil {
			t.Errorf("Expected error for test case %d", i)
		}
	}
}

func TestVolumesCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	classnameExclude := "^DCFILE.*"
	volumesClassnameExclude = &classnameExclude
	DsmadmcVolumesExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockVolumeStdout, nil
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="volumes"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="volumes"} 0
	# HELP tsm_volume_estimated_capacity_bytes Volume estimated capacity
	# TYPE tsm_volume_estimated_capacity_bytes gauge
	tsm_volume_estimated_capacity_bytes{classname="DCULT6E",volume="E00090L6"} 2512838983680
	tsm_volume_estimated_capacity_bytes{classname="DCULT7",volume="F00397L7"} 9507748970496
	tsm_volume_estimated_capacity_bytes{classname="DCULT7",volume="F00529L7"} 6013946167296
	tsm_volume_estimated_capacity_bytes{classname="DCULT7",volume="F00640L7"} 8597764308992
	# HELP tsm_volume_utilized_ratio Volume utilized ratio, 0.0-1.0
	# TYPE tsm_volume_utilized_ratio gauge
	tsm_volume_utilized_ratio{classname="DCULT6E",volume="E00090L6"} 1.00
	tsm_volume_utilized_ratio{classname="DCULT7",volume="F00397L7"} 0.43700000000000006
	tsm_volume_utilized_ratio{classname="DCULT7",volume="F00529L7"} 0.946
	tsm_volume_utilized_ratio{classname="DCULT7",volume="F00640L7"} 0.685
    # HELP tsm_volumes_readonly Number of readonly volumes
    # TYPE tsm_volumes_readonly gauge
    tsm_volumes_readonly 1
	# HELP tsm_volumes_unavailable Number of unavailable volumes
    # TYPE tsm_volumes_unavailable gauge
    tsm_volumes_unavailable 1
	`
	w := log.NewSyncWriter(os.Stderr)
	logger := log.NewLogfmtLogger(w)
	collector := NewVolumesExporter(&config.Target{}, logger)
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 13 {
		t.Errorf("Unexpected collection count %d, expected 13", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_volumes_unavailable", "tsm_volumes_readonly", "tsm_volume_estimated_capacity_bytes", "tsm_volume_utilized_ratio",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestVolumesCollectorError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcVolumesExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
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
	collector := NewVolumesExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_volumes_unavailable", "tsm_volumes_readonly",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestVolumesCollectorTimeout(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcVolumesExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
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
	collector := NewVolumesExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_volumes_unavailable", "tsm_volumes_readonly",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestDsmadmcVolumes(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 0
	mockedStdout = "foo"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dsmadmcVolumes(&config.Target{}, ctx, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != mockedStdout {
		t.Errorf("Unexpected out: %s", out)
	}
}
