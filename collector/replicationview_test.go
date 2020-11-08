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
	mockReplicationViewCompletedStdout = `
TEST2DB2,/TEST2CONF,2020-03-23 00:45:29.000000,2020-03-23 06:06:45.000000,2,167543418
TEST2DB2,/TEST4,2020-03-23 00:45:29.000000,2020-03-23 06:06:45.000000,2,1052637876956
`
	mockReplicationViewNotCompletedStdout = `
FOO,/BAR
BAR,/BAZ
`
)

func TestBuildReplicationViewCompletedQuery(t *testing.T) {
	expectedQuery := "SELECT NODE_NAME, FSNAME, START_TIME, END_TIME, TOTFILES_REPLICATED, TOTBYTES_REPLICATED FROM replicationview WHERE COMP_STATE = 'COMPLETE' ORDER BY END_TIME DESC"
	query := buildReplicationViewCompletedQuery(&config.Target{Name: "test"})
	if query != expectedQuery {
		t.Errorf("\nExpected: %s\nGot: %s", expectedQuery, query)
	}
	expectedQuery = "SELECT NODE_NAME, FSNAME, START_TIME, END_TIME, TOTFILES_REPLICATED, TOTBYTES_REPLICATED FROM replicationview WHERE NODE_NAME IN ('FOO','BAR') AND COMP_STATE = 'COMPLETE' ORDER BY END_TIME DESC"
	query = buildReplicationViewCompletedQuery(&config.Target{Name: "test", ReplicationNodeNames: []string{"FOO", "BAR"}})
	if query != expectedQuery {
		t.Errorf("\nExpected: %s\nGot: %s", expectedQuery, query)
	}
}

func TestBuildReplicationViewNotCompletedQuery(t *testing.T) {
	mockNow, _ := time.Parse("01/02/2006 15:04:05", "07/02/2020 13:00:00")
	timeNow = func() time.Time {
		return mockNow
	}
	expectedQuery := "SELECT NODE_NAME, FSNAME FROM replicationview WHERE COMP_STATE <> 'COMPLETE' AND DATE(START_TIME) BETWEEN '2020-07-01' AND '2020-07-02'"
	query := buildReplicationViewNotCompletedQuery(&config.Target{Name: "test"})
	if query != expectedQuery {
		t.Errorf("\nExpected: %s\nGot: %s", expectedQuery, query)
	}
	expectedQuery = "SELECT NODE_NAME, FSNAME FROM replicationview WHERE NODE_NAME IN ('FOO','BAR') AND COMP_STATE <> 'COMPLETE' AND DATE(START_TIME) BETWEEN '2020-07-01' AND '2020-07-02'"
	query = buildReplicationViewNotCompletedQuery(&config.Target{Name: "test", ReplicationNodeNames: []string{"FOO", "BAR"}})
	if query != expectedQuery {
		t.Errorf("\nExpected: %s\nGot: %s", expectedQuery, query)
	}
}

func TestReplicationViewsParse(t *testing.T) {
	metrics := replicationviewParse(mockReplicationViewCompletedStdout, mockReplicationViewNotCompletedStdout, log.NewNopLogger())
	if len(metrics) != 4 {
		t.Errorf("Expected 4 metrics, got %d", len(metrics))
		return
	}
	if val := metrics["TEST2DB2-/TEST2CONF"].NotCompleted; val != 0 {
		t.Errorf("Expected 0 notCompleted, got %v", val)
	}
	if val := metrics["TEST2DB2-/TEST2CONF"].Duration; val != 19276 {
		t.Errorf("Expected 19276 duration, got %v", val)
	}
	if val := metrics["FOO-/BAR"].NotCompleted; val != 1 {
		t.Errorf("Expected 1 notCompleted, got %v", val)
	}
	if val := metrics["BAR-/BAZ"].NotCompleted; val != 1 {
		t.Errorf("Expected 1 notCompleted, got %v", val)
	}
}

func TestReplicationViewsCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcReplicationViewsCompletedExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockReplicationViewCompletedStdout, nil
	}
	DsmadmcReplicationViewsNotCompletedExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockReplicationViewNotCompletedStdout, nil
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
	tsm_replication_duration_seconds{fsname="/BAR",nodename="FOO"} 0
	tsm_replication_duration_seconds{fsname="/BAZ",nodename="BAR"} 0
	# HELP tsm_replication_not_completed Number of replications not completed for today
	# TYPE tsm_replication_not_completed gauge
	tsm_replication_not_completed{fsname="/TEST2CONF",nodename="TEST2DB2"} 0
	tsm_replication_not_completed{fsname="/TEST4",nodename="TEST2DB2"} 0
	tsm_replication_not_completed{fsname="/BAR",nodename="FOO"} 1
	tsm_replication_not_completed{fsname="/BAZ",nodename="BAR"} 1
	# HELP tsm_replication_replicated_bytes Amount of data replicated in bytes
	# TYPE tsm_replication_replicated_bytes gauge
	tsm_replication_replicated_bytes{fsname="/TEST2CONF",nodename="TEST2DB2"} 167543418
	tsm_replication_replicated_bytes{fsname="/TEST4",nodename="TEST2DB2"} 1052637876956
	tsm_replication_replicated_bytes{fsname="/BAR",nodename="FOO"} 0
	tsm_replication_replicated_bytes{fsname="/BAZ",nodename="BAR"} 0
	# HELP tsm_replication_replicated_files Number of files replicated
	# TYPE tsm_replication_replicated_files gauge
	tsm_replication_replicated_files{fsname="/TEST2CONF",nodename="TEST2DB2"} 2
	tsm_replication_replicated_files{fsname="/TEST4",nodename="TEST2DB2"} 2
	tsm_replication_replicated_files{fsname="/BAR",nodename="FOO"} 0
	tsm_replication_replicated_files{fsname="/BAZ",nodename="BAR"} 0
	`
	collector := NewReplicationViewsExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 27 {
		t.Errorf("Unexpected collection count %d, expected 27", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_replication_duration_seconds", "tsm_replication_not_completed",
		"tsm_replication_replicated_bytes", "tsm_replication_replicated_files",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestReplicationViewsCollectorError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcReplicationViewsCompletedExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	DsmadmcReplicationViewsNotCompletedExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
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
	collector := NewReplicationViewsExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_replication_duration_seconds", "tsm_replication_not_completed",
		"tsm_replication_replicated_bytes", "tsm_replication_replicated_files",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestReplicationViewsCollectorTimeout(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcReplicationViewsCompletedExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", context.DeadlineExceeded
	}
	DsmadmcReplicationViewsNotCompletedExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
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
	collector := NewReplicationViewsExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_replication_duration_seconds", "tsm_replication_not_completed",
		"tsm_replication_replicated_bytes", "tsm_replication_replicated_files",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestDsmadmcReplicationViewsCompleted(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 0
	mockedStdout = "foo"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dsmadmcReplicationViewsCompleted(&config.Target{}, ctx, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != mockedStdout {
		t.Errorf("Unexpected out: %s", out)
	}
}

func TestDsmadmcReplicationViewsNotCompleted(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 0
	mockedStdout = "foo"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dsmadmcReplicationViewsNotCompleted(&config.Target{}, ctx, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != mockedStdout {
		t.Errorf("Unexpected out: %s", out)
	}
}
