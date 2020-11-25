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
	// query: SELECT FILESPACE_NAME,LOGICAL_MB,NODE_NAME,NUM_FILES,PHYSICAL_MB,STGPOOL_NAME FROM occupancy
	mockOccupancyStdout = `
/home,59.94,NETAPPUSER,3,59.94,PFNETAPP
/fs/project,1805773220.21,PROJECT,316487756,1806568784.42,PTGPFS
`
)

func TestOccupancysParse(t *testing.T) {
	metrics := occupancyParse(mockOccupancyStdout, log.NewNopLogger())
	if len(metrics) != 2 {
		t.Errorf("Expected 2 metrics, got %v", len(metrics))
	}
}

func TestOccupancysCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcOccupancysExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockOccupancyStdout, nil
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="occupancy"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="occupancy"} 0
	# HELP tsm_occupancy_files Number of files
	# TYPE tsm_occupancy_files gauge
	tsm_occupancy_files{filespace="/fs/project",nodename="PROJECT",storagepool="PTGPFS"} 316487756
	tsm_occupancy_files{filespace="/home",nodename="NETAPPUSER",storagepool="PFNETAPP"} 3
	# HELP tsm_occupancy_logical_bytes Logical space occupied
	# TYPE tsm_occupancy_logical_bytes gauge
	tsm_occupancy_logical_bytes{filespace="/fs/project",nodename="PROJECT",storagepool="PTGPFS"} 1893490460154920.96
	tsm_occupancy_logical_bytes{filespace="/home",nodename="NETAPPUSER",storagepool="PFNETAPP"} 62851645.44
	# HELP tsm_occupancy_physical_bytes Physical space occupied
	# TYPE tsm_occupancy_physical_bytes gauge
	tsm_occupancy_physical_bytes{filespace="/fs/project",nodename="PROJECT",storagepool="PTGPFS"} 1894324669691986
	tsm_occupancy_physical_bytes{filespace="/home",nodename="NETAPPUSER",storagepool="PFNETAPP"} 62851645.44
	`
	collector := NewOccupancysExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 9 {
		t.Errorf("Unexpected collection count %d, expected 9", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_occupancy_files", "tsm_occupancy_logical_bytes", "tsm_occupancy_physical_bytes",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestOccupancysCollectorError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcOccupancysExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="occupancy"} 1
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="occupancy"} 0
	`
	collector := NewOccupancysExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_occupancy_files", "tsm_occupancy_logical_bytes", "tsm_occupancy_physical_bytes",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestOccupancysCollectorTimeout(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcOccupancysExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", context.DeadlineExceeded
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="occupancy"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="occupancy"} 1
	`
	collector := NewOccupancysExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_occupancy_files", "tsm_occupancy_logical_bytes", "tsm_occupancy_physical_bytes",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestDsmadmcOccupancys(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 0
	mockedStdout = "foo"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dsmadmcOccupancys(&config.Target{}, ctx, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != mockedStdout {
		t.Errorf("Unexpected out: %s", out)
	}
}
