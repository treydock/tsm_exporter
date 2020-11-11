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
	"strings"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/treydock/tsm_exporter/config"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	mockedLogStdout = "32426.00,32768.00,342.00\n"
)

func TestLogParse(t *testing.T) {
	metrics := logParse(mockedLogStdout, log.NewNopLogger())
	if val := metrics.Total; val != 34359738368 {
		t.Errorf("Unexpected Total, got %v", val)
	}
}

func TestLogCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcLogExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockedLogStdout, nil
	}
	expected := `
	# HELP tsm_active_log_free_bytes Active log free space in bytes
	# TYPE tsm_active_log_free_bytes gauge
	tsm_active_log_free_bytes 34001125376
	# HELP tsm_active_log_total_bytes Active log total space in bytes
	# TYPE tsm_active_log_total_bytes gauge
	tsm_active_log_total_bytes 34359738368
	# HELP tsm_active_log_used_bytes Active log used space in bytes
	# TYPE tsm_active_log_used_bytes gauge
	tsm_active_log_used_bytes 358612992
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="log"} 0
	`
	collector := NewLogExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 5 {
		t.Errorf("Unexpected collection count %d, expected 5", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_active_log_free_bytes", "tsm_active_log_total_bytes", "tsm_active_log_used_bytes",
		"tsm_exporter_collect_error"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestLogCollectorError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcLogExec = func(target *config.Target, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="log"} 1
	`
	collector := NewLogExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 2 {
		t.Errorf("Unexpected collection count %d, expected 2", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_active_log_free_bytes", "tsm_active_log_total_bytes", "tsm_active_log_used_bytes",
		"tsm_exporter_collect_error"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

/*func TestDsmadmcLog(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 0
	mockedStdout = "foo"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dsmadmcLog(&config.Target{}, ctx, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != mockedStdout {
		t.Errorf("Unexpected out: %s", out)
	}
}*/
