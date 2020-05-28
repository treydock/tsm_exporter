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

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/treydock/tsm_exporter/config"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	// QUERY: SELECT DEVCLASS,PCT_UTILIZED,POOLTYPE,STGPOOL_NAME,STG_TYPE FROM stgpools
	mockedStoragePoolStdout = `
DISK,0.0,PRIMARY,ARCHIVEPOOL,DEVCLASS
DCFILEE,41.8,PRIMARY,EPFESS,DEVCLASS
DCULT7,42.6,PRIMARY,PTGPFS,DEVCLASS
`
)

func TestStoragePoolParse(t *testing.T) {
	metrics := stgpoolsParse(mockedStoragePoolStdout, log.NewNopLogger())
	if len(metrics) != 3 {
		t.Errorf("Expected 3 metrics, got %v", len(metrics))
		return
	}
	if val := metrics[0].Name; val != "ARCHIVEPOOL" {
		t.Errorf("Unexpected name, got %v", val)
	}
}

func TestStoragePoolCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcStoragePoolExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockedStoragePoolStdout, nil
	}
	expected := `
	# HELP tsm_storage_pool_utilized_percent Storage pool utilized percent
	# TYPE tsm_storage_pool_utilized_percent gauge
	tsm_storage_pool_utilized_percent{classname="DISK",name="ARCHIVEPOOL",pooltype="PRIMARY",storagetype="DEVCLASS"} 0.0
	tsm_storage_pool_utilized_percent{classname="DCFILEE",name="EPFESS",pooltype="PRIMARY",storagetype="DEVCLASS"} 41.8
	tsm_storage_pool_utilized_percent{classname="DCULT7",name="PTGPFS",pooltype="PRIMARY",storagetype="DEVCLASS"} 42.6
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="stgpools"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="stgpools"} 0
	`
	w := log.NewSyncWriter(os.Stderr)
	logger := log.NewLogfmtLogger(w)
	collector := NewStoragePoolExporter(&config.Target{}, logger, false)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 6 {
		t.Errorf("Unexpected collection count %d, expected 6", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_storage_pool_utilized_percent",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestStoragePoolCollectorError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcStoragePoolExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="stgpools"} 1
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="stgpools"} 0
	`
	collector := NewStoragePoolExporter(&config.Target{}, log.NewNopLogger(), false)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_storage_pool_utilized_percent", "tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestStoragePoolCollectorTimeout(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcStoragePoolExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", context.DeadlineExceeded
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="stgpools"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="stgpools"} 1
	`
	collector := NewStoragePoolExporter(&config.Target{}, log.NewNopLogger(), false)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_storage_pool_utilized_percent", "tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestStoragePoolCollectorCache(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcStoragePoolExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockedStoragePoolStdout, nil
	}
	expected := `
	# HELP tsm_storage_pool_utilized_percent Storage pool utilized percent
	# TYPE tsm_storage_pool_utilized_percent gauge
	tsm_storage_pool_utilized_percent{classname="DISK",name="ARCHIVEPOOL",pooltype="PRIMARY",storagetype="DEVCLASS"} 0.0
	tsm_storage_pool_utilized_percent{classname="DCFILEE",name="EPFESS",pooltype="PRIMARY",storagetype="DEVCLASS"} 41.8
	tsm_storage_pool_utilized_percent{classname="DCULT7",name="PTGPFS",pooltype="PRIMARY",storagetype="DEVCLASS"} 42.6
	`
	errorMetric := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="stgpools"} 1
	`
	timeoutMetric := `
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="stgpools"} 1
	`
	collector := NewStoragePoolExporter(&config.Target{}, log.NewNopLogger(), true)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 6 {
		t.Errorf("Unexpected collection count %d, expected 6", val)
	}
	DsmadmcStoragePoolExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	if val := testutil.CollectAndCount(collector); val != 6 {
		t.Errorf("Unexpected collection count %d, expected 6", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(errorMetric+expected),
		"tsm_storage_pool_utilized_percent",
		"tsm_exporter_collect_error"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
	DsmadmcStoragePoolExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", context.DeadlineExceeded
	}
	if val := testutil.CollectAndCount(collector); val != 6 {
		t.Errorf("Unexpected collection count %d, expected 6", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(timeoutMetric+expected),
		"tsm_storage_pool_utilized_percent",
		"tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestDsmadmcStoragePool(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 0
	mockedStdout = "foo"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dsmadmcStoragePool(&config.Target{}, ctx, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != mockedStdout {
		t.Errorf("Unexpected out: %s", out)
	}
}
