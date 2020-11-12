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
	"os"
	"strings"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/treydock/tsm_exporter/config"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	mockedDBStdout = "88.6,TSMDB1,3092796,1453663,2020-05-22 08:10:00.000000,98.3,0,11607707032,28836868,2096672,28836092,642976,25743296\n"
)

func TestDBParse(t *testing.T) {
	metrics := dbParse(mockedDBStdout, log.NewNopLogger())
	if len(metrics) != 1 {
		t.Errorf("Expected 1 metrics, got %v", len(metrics))
		return
	}
	if val := metrics[0].Name; val != "TSMDB1" {
		t.Errorf("Unexpected name, got %v", val)
	}
}

func TestDBCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcDBExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockedDBStdout, nil
	}
	expected := `
	# HELP tsm_db_buffer_hit_ratio DB buffer hit ratio (0.0-1.0)
	# TYPE tsm_db_buffer_hit_ratio gauge
	tsm_db_buffer_hit_ratio{dbname="TSMDB1"} 0.8859999999999999
	# HELP tsm_db_buffer_requests_total DB total buffer requests
	# TYPE tsm_db_buffer_requests_total counter
	tsm_db_buffer_requests_total{dbname="TSMDB1"} 11607707032
	# HELP tsm_db_pages_free DB free pages
	# TYPE tsm_db_pages_free gauge
	# HELP tsm_db_last_backup_timestamp_seconds Time since last backup in epoch
	# TYPE tsm_db_last_backup_timestamp_seconds gauge
	tsm_db_last_backup_timestamp_seconds{dbname="TSMDB1"} 1.590135e+09
	tsm_db_pages_free{dbname="TSMDB1"} 3092796
	# HELP tsm_db_pages_total DB total pages
	# TYPE tsm_db_pages_total gauge
	tsm_db_pages_total{dbname="TSMDB1"} 28836868
	# HELP tsm_db_pages_usable DB usable pages
	# TYPE tsm_db_pages_usable gauge
	tsm_db_pages_usable{dbname="TSMDB1"} 28836092
	# HELP tsm_db_pages_used DB used pages
	# TYPE tsm_db_pages_used gauge
	tsm_db_pages_used{dbname="TSMDB1"} 25743296
	# HELP tsm_db_pkg_hit_ratio DB pkg hit ratio (0.0-1.0)
	# TYPE tsm_db_pkg_hit_ratio gauge
	tsm_db_pkg_hit_ratio{dbname="TSMDB1"} 0.983
	# HELP tsm_db_sort_overflow DB sort overflow
	# TYPE tsm_db_sort_overflow gauge
	tsm_db_sort_overflow{dbname="TSMDB1"} 0
    # HELP tsm_db_space_free_bytes DB free space in bytes
    # TYPE tsm_db_space_free_bytes gauge
    tsm_db_space_free_bytes{dbname="TSMDB1"} 1524276133888
    # HELP tsm_db_space_total_bytes DB total space in bytes
    # TYPE tsm_db_space_total_bytes gauge
    tsm_db_space_total_bytes{dbname="TSMDB1"} 2198519939072
    # HELP tsm_db_space_used_bytes DB used space in bytes
    # TYPE tsm_db_space_used_bytes gauge
    tsm_db_space_used_bytes{dbname="TSMDB1"} 674209202176
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="db"} 0
	`
	w := log.NewSyncWriter(os.Stderr)
	logger := log.NewLogfmtLogger(w)
	collector := NewDBExporter(&config.Target{}, logger)
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 14 {
		t.Errorf("Unexpected collection count %d, expected 14", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_db_space_total_bytes", "tsm_db_space_used_bytes", "tsm_db_space_free_bytes",
		"tsm_db_pages_total", "tsm_db_pages_usable", "tsm_db_pages_used", "tsm_db_pages_free",
		"tsm_db_buffer_hit_ratio", "tsm_db_buffer_requests_total", "tsm_db_sort_overflow", "tsm_db_pkg_hit_ratio",
		"tsm_db_last_backup_timestamp_seconds",
		"tsm_exporter_collect_error"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestDBCollectorError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcDBExec = func(target *config.Target, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="db"} 1
	`
	collector := NewDBExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 2 {
		t.Errorf("Unexpected collection count %d, expected 2", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_db_space_total_bytes", "tsm_exporter_collect_error"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

/*func TestDsmadmcDB(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 0
	mockedStdout = "foo"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dsmadmcDB(&config.Target{}, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != mockedStdout {
		t.Errorf("Unexpected out: %s", out)
	}
}*/
