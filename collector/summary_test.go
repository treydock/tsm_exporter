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
	mockSummaryStdout = `
Ignored,item
BACKUP,BCPDB-TEST_ENC,DAILY_BCPDB-TEST,1340416600,2020-12-05 01:01:26.000000
BACKUP,NETAPPUSER2 (SPCLIENT02.EXAMPLE.COM),,358279255408,2020-12-05 02:54:14.000000
BACKUP,BCPDB-TEST_ENC,DAILY_BCPDB-TEST,1340416600,2020-12-04 01:01:26.000000
`
)

func TestBuildSummaryQuery(t *testing.T) {
	expectedQuery := "SELECT ACTIVITY,ENTITY,SCHEDULE_NAME,SUM(BYTES),MAX(END_TIME) FROM SUMMARY_EXTENDED"
	expectedQuery = expectedQuery + " WHERE ACTIVITY NOT IN ('TAPE MOUNT','EXPIRATION','PROCESS_START','PROCESS_END') AND ACTIVITY NOT LIKE 'SUR_%'"
	expectedQuery = expectedQuery + " GROUP BY ACTIVITY,ENTITY,SCHEDULE_NAME,DATE(END_TIME) ORDER BY DATE(END_TIME) DESC"
	query := buildSummaryQuery(&config.Target{Name: "test"})
	if query != expectedQuery {
		t.Errorf("\nExpected: %s\nGot: %s", expectedQuery, query)
	}
	expectedQuery = "SELECT ACTIVITY,ENTITY,SCHEDULE_NAME,SUM(BYTES),MAX(END_TIME) FROM SUMMARY_EXTENDED"
	expectedQuery = expectedQuery + " WHERE ACTIVITY IN ('BACKUP','REPLICATION')"
	expectedQuery = expectedQuery + " GROUP BY ACTIVITY,ENTITY,SCHEDULE_NAME,DATE(END_TIME) ORDER BY DATE(END_TIME) DESC"
	query = buildSummaryQuery(&config.Target{Name: "test", SummaryActivities: []string{"BACKUP", "REPLICATION"}})
	if query != expectedQuery {
		t.Errorf("\nExpected: %s\nGot: %s", expectedQuery, query)
	}
}

func TestSummaryParse(t *testing.T) {
	metrics, err := summaryParse(mockSummaryStdout, &config.Target{}, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		return
	}
	if len(metrics) != 2 {
		t.Errorf("Expected 2 metrics, got %v", len(metrics))
	}
}

func TestSummaryParseErrors(t *testing.T) {
	tests := []string{
		"BACKUP,BCPDB-TEST_ENC,DAILY_BCPDB-TEST,foo,2020-12-05 01:01:26.000000\n",
		"BACKUP,BCPDB-TEST_ENC,DAILY_BCPDB-TEST,1340416600,foo",
		"BACKUP,BCPDB-TEST_ENC,\"DAILY_BCPDB-TEST,1340416600,2020-12-05 01:01:26.000000\n",
	}
	for i, out := range tests {
		_, err := summaryParse(out, &config.Target{}, log.NewNopLogger())
		if err == nil {
			t.Errorf("Expected error in test case %d", i)
		}
	}
}

func TestSummaryCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	zone := "America/New_York"
	timezone = &zone
	DsmadmcSummaryExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockSummaryStdout, nil
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="summary"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="summary"} 0
	# HELP tsm_summary_bytes Amount of data backed up in last 24 hours
	# TYPE tsm_summary_bytes gauge
	tsm_summary_bytes{activity="BACKUP",entity="BCPDB-TEST_ENC",schedule="DAILY_BCPDB-TEST"} 1340416600
	tsm_summary_bytes{activity="BACKUP",entity="NETAPPUSER2 (SPCLIENT02.EXAMPLE.COM)",schedule=""} 358279255408
	# HELP tsm_summary_end_timestamp_seconds End time of last backup
	# TYPE tsm_summary_end_timestamp_seconds gauge
	tsm_summary_end_timestamp_seconds{activity="BACKUP",entity="BCPDB-TEST_ENC",schedule="DAILY_BCPDB-TEST"} 1607148086
	tsm_summary_end_timestamp_seconds{activity="BACKUP",entity="NETAPPUSER2 (SPCLIENT02.EXAMPLE.COM)",schedule=""} 1607154854
	`
	collector := NewSummaryExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 7 {
		t.Errorf("Unexpected collection count %d, expected 7", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_summary_bytes", "tsm_summary_end_timestamp_seconds",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestSummaryCollectorError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcSummaryExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="summary"} 1
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="summary"} 0
	`
	collector := NewSummaryExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_summary_bytes", "tsm_summary_end_timestamp_seconds",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestSummaryCollectorTimeout(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcSummaryExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", context.DeadlineExceeded
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="summary"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="summary"} 1
	`
	collector := NewSummaryExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_summary_bytes", "tsm_summary_end_timestamp_seconds",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestDsmadmcSummary(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 0
	mockedStdout = "foo"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dsmadmcSummary(&config.Target{}, ctx, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != mockedStdout {
		t.Errorf("Unexpected out: %s", out)
	}
}
