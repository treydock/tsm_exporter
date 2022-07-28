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

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/treydock/tsm_exporter/config"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	mockReplicationViewStdout = `
Data,to,ignore
TEST2DB2,/TEST2CONF,2020-03-23 00:45:29.000000,2020-03-23 06:06:45.000000,2,167543418,COMPLETE
TEST2DB2,/TEST2CONF,2020-03-22 00:43:29.000000,2020-03-22 06:06:45.000000,2,167543418,COMPLETE
TEST2DB2,/TEST4,2020-03-23 00:45:29.000000,2020-03-23 06:06:45.000000,2,1052637876956,COMPLETE
NETAPPUSER4_ENC,/users/PSS0004,2022-07-26 09:59:12.000000,1900-01-01 00:00:00.000000,208,0,INCOMPLETE
`
)

func TestBuildReplicationViewQuery(t *testing.T) {
	expectedQuery := "SELECT NODE_NAME, FSNAME, START_TIME, END_TIME, TOTFILES_REPLICATED, TOTBYTES_REPLICATED, COMP_STATE FROM replicationview ORDER BY END_TIME DESC"
	query := buildReplicationViewQuery(&config.Target{Name: "test"})
	if query != expectedQuery {
		t.Errorf("\nExpected: %s\nGot: %s", expectedQuery, query)
	}
	expectedQuery = "SELECT NODE_NAME, FSNAME, START_TIME, END_TIME, TOTFILES_REPLICATED, TOTBYTES_REPLICATED, COMP_STATE FROM replicationview WHERE NODE_NAME IN ('FOO','BAR') ORDER BY END_TIME DESC"
	query = buildReplicationViewQuery(&config.Target{Name: "test", ReplicationNodeNames: []string{"FOO", "BAR"}})
	if query != expectedQuery {
		t.Errorf("\nExpected: %s\nGot: %s", expectedQuery, query)
	}
}

func TestReplicationViewParse(t *testing.T) {
	metrics, err := replicationviewParse(mockReplicationViewStdout, &config.Target{}, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		return
	}
	if len(metrics) != 3 {
		t.Errorf("Expected 4 metrics, got %d", len(metrics))
		return
	}
	if val := metrics["TEST2DB2-/TEST2CONF-COMPLETE"].CompState; val != "COMPLETE" {
		t.Errorf("Expected COMPLETE, got %v", val)
	}
	if val := metrics["TEST2DB2-/TEST2CONF-COMPLETE"].Duration; val != 19276 {
		t.Errorf("Expected 19276 duration, got %v", val)
	}
	if val := metrics["NETAPPUSER4_ENC-/users/PSS0004-INCOMPLETE"].CompState; val != "INCOMPLETE" {
		t.Errorf("Expected INCOMPLETE, got %v", val)
	}
}

func TestReplicationViewParseErrors(t *testing.T) {
	tests := []string{
		"TEST2DB2,/TEST2CONF,FOO,2020-03-23 06:06:45.000000,2,167543418,COMPLETE\n",
		"TEST2DB2,/TEST2CONF,2020-03-23 00:45:29.000000,FOO,2,167543418,COMPLETE\n",
		"TEST2DB2,/TEST2CONF,2020-03-23 00:45:29.000000,2020-03-23 06:06:45.000000,FOO,167543418,COMPLETE\n",
		"TEST2DB2,/TEST2CONF,2020-03-23 00:45:29.000000,2020-03-23 06:06:45.000000,2,FOO,COMPLETE\n",
		"TEST2DB2,/TEST2CONF,\"2020-03-23\" 00:45:29.000000\",2020-03-23 06:06:45.000000,2,167543418,COMPLETE",
	}
	for i, out := range tests {
		_, err := replicationviewParse(out, &config.Target{}, log.NewNopLogger())
		if err == nil {
			t.Errorf("Expected error on test case %d", i)
		}
	}
}

func TestReplicationViewCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcReplicationViewExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockReplicationViewStdout, nil
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="replicationview"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="replicationview"} 0
	# HELP tsm_replication_duration_seconds Amount of time taken to complete the most recent replication
	# TYPE tsm_replication_duration_seconds gauge
	tsm_replication_duration_seconds{fsname="/TEST2CONF",nodename="TEST2DB2"} 19276
	tsm_replication_duration_seconds{fsname="/TEST4",nodename="TEST2DB2"} 19276
	# HELP tsm_replication_end_timestamp_seconds End time of replication
	# TYPE tsm_replication_end_timestamp_seconds gauge
	tsm_replication_end_timestamp_seconds{fsname="/TEST2CONF",nodename="TEST2DB2"} 1584958005
	tsm_replication_end_timestamp_seconds{fsname="/TEST4",nodename="TEST2DB2"} 1584958005
	# HELP tsm_replication_incomplete_replicated_files Number of files replicated for incomplete
	# TYPE tsm_replication_incomplete_replicated_files gauge
	tsm_replication_incomplete_replicated_files{fsname="/users/PSS0004",nodename="NETAPPUSER4_ENC"} 208
    # HELP tsm_replication_incomplete_start_timestamp_seconds Start time of incomplete replication
    # TYPE tsm_replication_incomplete_start_timestamp_seconds gauge
    tsm_replication_incomplete_start_timestamp_seconds{fsname="/users/PSS0004",nodename="NETAPPUSER4_ENC"} 1658843952
	# HELP tsm_replication_replicated_bytes Amount of data replicated in bytes
	# TYPE tsm_replication_replicated_bytes gauge
	tsm_replication_replicated_bytes{fsname="/TEST2CONF",nodename="TEST2DB2"} 167543418
	tsm_replication_replicated_bytes{fsname="/TEST4",nodename="TEST2DB2"} 1052637876956
	# HELP tsm_replication_replicated_files Number of files replicated
	# TYPE tsm_replication_replicated_files gauge
	tsm_replication_replicated_files{fsname="/TEST2CONF",nodename="TEST2DB2"} 2
	tsm_replication_replicated_files{fsname="/TEST4",nodename="TEST2DB2"} 2
	# HELP tsm_replication_start_timestamp_seconds Start time of replication
	# TYPE tsm_replication_start_timestamp_seconds gauge
	tsm_replication_start_timestamp_seconds{fsname="/TEST2CONF",nodename="TEST2DB2"} 1584938729
	tsm_replication_start_timestamp_seconds{fsname="/TEST4",nodename="TEST2DB2"} 1584938729
	`
	zone := "America/New_York"
	timezone = &zone
	collector := NewReplicationViewExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 15 {
		t.Errorf("Unexpected collection count %d, expected 15", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_replication_duration_seconds",
		"tsm_replication_replicated_bytes", "tsm_replication_replicated_files", "tsm_replication_incomplete_replicated_files",
		"tsm_replication_start_timestamp_seconds", "tsm_replication_incomplete_start_timestamp_seconds",
		"tsm_replication_end_timestamp_seconds",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestReplicationViewCollectorError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcReplicationViewExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="replicationview"} 1
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="replicationview"} 0
	`
	collector := NewReplicationViewExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_replication_duration_seconds",
		"tsm_replication_replicated_bytes", "tsm_replication_replicated_files", "tsm_replication_incomplete_replicated_files",
		"tsm_replication_start_timestamp_seconds", "tsm_replication_incomplete_start_timestamp_seconds",
		"tsm_replication_end_timestamp_seconds",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestReplicationViewCollectorTimeout(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcReplicationViewExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", context.DeadlineExceeded
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="replicationview"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="replicationview"} 1
	`
	collector := NewReplicationViewExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_replication_duration_seconds",
		"tsm_replication_replicated_bytes", "tsm_replication_replicated_files", "tsm_replication_incomplete_replicated_files",
		"tsm_replication_start_timestamp_seconds", "tsm_replication_incomplete_start_timestamp_seconds",
		"tsm_replication_end_timestamp_seconds",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestDsmadmcReplicationView(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 0
	mockedStdout = "foo"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dsmadmcReplicationView(&config.Target{}, ctx, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != mockedStdout {
		t.Errorf("Unexpected out: %s", out)
	}
}
