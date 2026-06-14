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
	mockProcessStdout = `
100,Replicate Node,311889684,0,
314,Migration,398582,234399813418,
395,Space Reclamation,2011,321164298555,
400,Replicate Node,150000,100000000,
401,Replicate Node,50000,200000000,
500,Migration,10000,150000000,
600,Backup,5000,75000000,
700,Backup,3000,45000000,
800,Space Reclamation,100,1000000,
`
)

func TestProcessParse(t *testing.T) {
	metrics, err := processParse(mockProcessStdout, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		return
	}
	if val := len(metrics); val != 9 {
		t.Errorf("Expected 7 metrics, got %d", val)
	}
	if val := metrics[0].name; val != "Replicate Node" {
		t.Errorf("Expected process name 'Replicate Node', got %s", val)
	}
	if val := metrics[0].filesProcessed; val != 311889684 {
		t.Errorf("Expected files_processed 311889684, got %v", val)
	}
	if val := metrics[0].bytesProcessed; val != 0 {
		t.Errorf("Expected bytes_processed 0, got %v", val)
	}
	// Verify that we have multiple processes with the same name (Replicate Node)
	// and that the data is properly grouped
	replicateNodes := 0
	totalFiles := 0.0
	totalBytes := 0.0
	for _, metric := range metrics {
		if metric.name == "Replicate Node" {
			replicateNodes++
			totalFiles += metric.filesProcessed
			totalBytes += metric.bytesProcessed
		}
	}
	if replicateNodes != 3 {
		t.Errorf("Expected 3 Replicate Node processes, got %d", replicateNodes)
	}
	if totalFiles != 311889684+150000+50000 {
		t.Errorf("Expected total files 311889684+150000+50000, got %v", totalFiles)
	}
	if totalBytes != 0+100000000+200000000 {
		t.Errorf("Expected total bytes 0+100000000+200000000, got %v", totalBytes)
	}
}

func TestProcessParseError(t *testing.T) {
	tests := []string{
		"100,Replicate Node,foo,0,\n",
		"100,Replicate Node,123,bar,\n",
	}
	for i, out := range tests {
		_, err := processParse(out, log.NewNopLogger())
		if err == nil {
			t.Errorf("Expected error for test case %d", i)
		}
	}
}

func TestProcessCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcProcessExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockProcessStdout, nil
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="process"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="process"} 0
	# HELP tsm_process_count Number of processes
	# TYPE tsm_process_count gauge
	tsm_process_count{process="Backup"} 2
	tsm_process_count{process="Migration"} 2
	tsm_process_count{process="Replicate Node"} 3
	tsm_process_count{process="Space Reclamation"} 2
	# HELP tsm_process_processed_bytes Total bytes processed by process
	# TYPE tsm_process_processed_bytes gauge
	tsm_process_processed_bytes{process="Backup"} 120000000
	tsm_process_processed_bytes{process="Migration"} 234549813418
	tsm_process_processed_bytes{process="Replicate Node"} 300000000
	tsm_process_processed_bytes{process="Space Reclamation"} 321165298555
	# HELP tsm_process_processed_files Total files processed by process
	# TYPE tsm_process_processed_files gauge
	tsm_process_processed_files{process="Backup"} 8000
	tsm_process_processed_files{process="Migration"} 408582
	tsm_process_processed_files{process="Replicate Node"} 312089684
	tsm_process_processed_files{process="Space Reclamation"} 2111
	`
	w := log.NewSyncWriter(os.Stderr)
	logger := log.NewLogfmtLogger(w)
	collector := NewProcessExporter(&config.Target{}, logger)
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 15 {
		t.Errorf("Unexpected collection count %d, expected 15", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_process_count", "tsm_process_processed_bytes", "tsm_process_processed_files",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestProcessCollectorError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcProcessExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="process"} 1
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="process"} 0
	`
	collector := NewProcessExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestProcessCollectorTimeout(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcProcessExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", context.DeadlineExceeded
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="process"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="process"} 1
	`
	collector := NewProcessExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestDsmadmcProcess(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 0
	mockedStdout = "foo"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dsmadmcProcess(&config.Target{}, ctx, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != mockedStdout {
		t.Errorf("Unexpected out: %s", out)
	}
}
