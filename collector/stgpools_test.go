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
	// QUERY: SELECT DEVCLASS,EST_CAPACITY_MB,LOCAL_EST_CAPACITY_MB,LOCAL_PCT_LOGICAL,LOCAL_PCT_UTILIZED,PCT_LOGICAL,PCT_UTILIZED,POOLTYPE,STGPOOL_NAME,STG_TYPE,TOTAL_CLOUD_SPACE_MB,USED_CLOUD_SPACE_MB FROM stgpools
	mockedStoragePoolStdout = `
Data,to,ignore
DISK,0.0,,,,100.0,0.0,PRIMARY,ARCHIVEPOOL,DEVCLASS,,
DCFILEE,25608540.0,,,,100.0,41.8,PRIMARY,EPFESS,DEVCLASS,,
DCULT7,3199882345.5,,,,99.7,42.6,PRIMARY,PTGPFS,DEVCLASS,,
,,"897775,0","100,0","0,0",,,PRIMARY,CLOUDTSMAZ,CLOUD,130,128
`
)

func TestStoragePoolParse(t *testing.T) {
	metrics, err := stgpoolsParse(mockedStoragePoolStdout, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		return
	}
	if len(metrics) != 4 {
		t.Errorf("Expected 4 metrics, got %v", len(metrics))
		return
	}
	if val := metrics[0].Name; val != "ARCHIVEPOOL" {
		t.Errorf("Unexpected name, got %v", val)
	}
	if val := metrics[1].PercentUtilized; val != 0.418 {
		t.Errorf("Unexpected PercentUtilized, got %v", val)
	}
}

func TestStoragePoolParseErrors(t *testing.T) {
	tests := []string{
		"DISK,0.0,,,,100.0,FOO,PRIMARY,ARCHIVEPOOL,DEVCLASS,,\n",
		"DISK,0.0,,\",,100.0,0.0,PRIMARY,ARCHIVEPOOL,DEVCLASS,,\n",
	}
	for i, out := range tests {
		_, err := stgpoolsParse(out, log.NewNopLogger())
		if err == nil {
			t.Errorf("Expected error in test case %d", i)
		}
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
	# HELP tsm_storage_pool_cloud_total_bytes Storage pool total cloud space
	# TYPE tsm_storage_pool_cloud_total_bytes gauge
	tsm_storage_pool_cloud_total_bytes{classname="",pooltype="PRIMARY",storagepool="CLOUDTSMAZ",storagetype="CLOUD"} 136314880
	# HELP tsm_storage_pool_cloud_used_bytes Storage pool used cloud space
	# TYPE tsm_storage_pool_cloud_used_bytes gauge
	tsm_storage_pool_cloud_used_bytes{classname="",pooltype="PRIMARY",storagepool="CLOUDTSMAZ",storagetype="CLOUD"} 134217728
	# HELP tsm_storage_pool_estimated_capacity_bytes Storage pool estimated capacity
	# TYPE tsm_storage_pool_estimated_capacity_bytes gauge
	tsm_storage_pool_estimated_capacity_bytes{classname="DISK",pooltype="PRIMARY",storagepool="ARCHIVEPOOL",storagetype="DEVCLASS"} 0.0
	tsm_storage_pool_estimated_capacity_bytes{classname="DCFILEE",pooltype="PRIMARY",storagepool="EPFESS",storagetype="DEVCLASS"} 26852500439040
	tsm_storage_pool_estimated_capacity_bytes{classname="DCULT7",pooltype="PRIMARY",storagepool="PTGPFS",storagetype="DEVCLASS"} 3355319830315008
	# HELP tsm_storage_pool_local_estimated_capacity_bytes Storage pool local estimated capacity
	# TYPE tsm_storage_pool_local_estimated_capacity_bytes gauge
	tsm_storage_pool_local_estimated_capacity_bytes{classname="",pooltype="PRIMARY",storagepool="CLOUDTSMAZ",storagetype="CLOUD"} 941385318400
	# HELP tsm_storage_pool_local_logical_ratio Storage pool local logical occupancy ratio, 0.0-1.0
	# TYPE tsm_storage_pool_local_logical_ratio gauge
	tsm_storage_pool_local_logical_ratio{classname="",pooltype="PRIMARY",storagepool="CLOUDTSMAZ",storagetype="CLOUD"} 1.0
	# HELP tsm_storage_pool_local_utilized_ratio Storage pool local utilized ratio, 0.0-1.0
	# TYPE tsm_storage_pool_local_utilized_ratio gauge
	tsm_storage_pool_local_utilized_ratio{classname="",pooltype="PRIMARY",storagepool="CLOUDTSMAZ",storagetype="CLOUD"} 0
	# HELP tsm_storage_pool_logical_ratio Storage pool logical occupancy ratio, 0.0-1.0
	# TYPE tsm_storage_pool_logical_ratio gauge
	tsm_storage_pool_logical_ratio{classname="DISK",pooltype="PRIMARY",storagepool="ARCHIVEPOOL",storagetype="DEVCLASS"} 1.0
	tsm_storage_pool_logical_ratio{classname="DCFILEE",pooltype="PRIMARY",storagepool="EPFESS",storagetype="DEVCLASS"} 1.0
	tsm_storage_pool_logical_ratio{classname="DCULT7",pooltype="PRIMARY",storagepool="PTGPFS",storagetype="DEVCLASS"} 0.997
	# HELP tsm_storage_pool_utilized_ratio Storage pool utilized ratio, 0.0-1.0
	# TYPE tsm_storage_pool_utilized_ratio gauge
	tsm_storage_pool_utilized_ratio{classname="DISK",pooltype="PRIMARY",storagepool="ARCHIVEPOOL",storagetype="DEVCLASS"} 0.0
	tsm_storage_pool_utilized_ratio{classname="DCFILEE",pooltype="PRIMARY",storagepool="EPFESS",storagetype="DEVCLASS"} 0.418
	tsm_storage_pool_utilized_ratio{classname="DCULT7",pooltype="PRIMARY",storagepool="PTGPFS",storagetype="DEVCLASS"} 0.426
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="stgpools"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="stgpools"} 0
	`
	w := log.NewSyncWriter(os.Stderr)
	logger := log.NewLogfmtLogger(w)
	collector := NewStoragePoolExporter(&config.Target{}, logger)
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 17 {
		t.Errorf("Unexpected collection count %d, expected 17", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_storage_pool_cloud_total_bytes", "tsm_storage_pool_cloud_used_bytes",
		"tsm_storage_pool_estimated_capacity_bytes", "tsm_storage_pool_local_estimated_capacity_bytes",
		"tsm_storage_pool_local_logical_ratio", "tsm_storage_pool_local_utilized_ratio",
		"tsm_storage_pool_logical_ratio", "tsm_storage_pool_utilized_ratio",
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
	collector := NewStoragePoolExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_storage_pool_utilized_ratio", "tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
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
	collector := NewStoragePoolExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_storage_pool_utilized_percent", "tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
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
