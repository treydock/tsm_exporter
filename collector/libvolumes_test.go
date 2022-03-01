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
	mockLibVolumeStdout = `
LTO-5,Private,LIB1,147
LTO-6,Private,LIB1,573
LTO-6,Scratch,LIB1,365
LTO-7,Private,LIB1,1082
LTO-7,Scratch,LIB1,153
`
)

func TestBuildLibVolumeQuery(t *testing.T) {
	expectedQuery := "SELECT MEDIATYPE,STATUS,LIBRARY_NAME,COUNT(*) FROM libvolumes GROUP BY(MEDIATYPE,STATUS,LIBRARY_NAME)"
	query := buildLibVolumesQuery(&config.Target{Name: "test"})
	if query != expectedQuery {
		t.Errorf("\nExpected: %s\nGot: %s", expectedQuery, query)
	}
	expectedQuery = "SELECT MEDIATYPE,STATUS,LIBRARY_NAME,COUNT(*) FROM libvolumes WHERE LIBRARY_NAME='LIB1' GROUP BY(MEDIATYPE,STATUS,LIBRARY_NAME)"
	query = buildLibVolumesQuery(&config.Target{Name: "test", LibraryName: "LIB1"})
	if query != expectedQuery {
		t.Errorf("\nExpected: %s\nGot: %s", expectedQuery, query)
	}
}

func TestLibVolumesParse(t *testing.T) {
	metrics, err := libvolumesParse(mockLibVolumeStdout, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		return
	}
	if len(metrics) != 3 {
		t.Errorf("Expected 3 metrics, got %v", len(metrics))
	}
}

func TestLibVolumesParseErrors(t *testing.T) {
	tests := []string{
		"LTO-5,Private,LIB1,foo\n",
		"LTO-5,\"Private\"\",LIB1,147",
		"LTO-7,Foo,LIB1,153\n",
	}
	for i, out := range tests {
		_, err := libvolumesParse(out, log.NewNopLogger())
		if err == nil {
			t.Errorf("Expected error in test case %d", i)
		}
	}
}

func TestLibVolumesCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcLibVolumesExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockLibVolumeStdout, nil
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="libvolumes"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="libvolumes"} 0
	# HELP tsm_libvolume_media Number of tapes
	# TYPE tsm_libvolume_media gauge
	tsm_libvolume_media{library="LIB1",mediatype="LTO-5",status="private"} 147
	tsm_libvolume_media{library="LIB1",mediatype="LTO-5",status="scratch"} 0
	tsm_libvolume_media{library="LIB1",mediatype="LTO-6",status="private"} 573
	tsm_libvolume_media{library="LIB1",mediatype="LTO-6",status="scratch"} 365
	tsm_libvolume_media{library="LIB1",mediatype="LTO-7",status="private"} 1082
	tsm_libvolume_media{library="LIB1",mediatype="LTO-7",status="scratch"} 153
	`
	collector := NewLibVolumesExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 9 {
		t.Errorf("Unexpected collection count %d, expected 8", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_libvolume_media",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestLibVolumesCollectorError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcLibVolumesExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="libvolumes"} 1
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="libvolumes"} 0
	`
	collector := NewLibVolumesExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_libvolume_media",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestLibVolumesCollectorTimeout(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcLibVolumesExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", context.DeadlineExceeded
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="libvolumes"} 0
    # HELP tsm_exporter_collect_timeout Indicates the collector timed out
    # TYPE tsm_exporter_collect_timeout gauge
    tsm_exporter_collect_timeout{collector="libvolumes"} 1
	`
	collector := NewLibVolumesExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 3 {
		t.Errorf("Unexpected collection count %d, expected 3", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_libvolume_media",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestDsmadmcLibVolumes(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 0
	mockedStdout = "foo"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dsmadmcLibVolumes(&config.Target{}, ctx, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != mockedStdout {
		t.Errorf("Unexpected out: %s", out)
	}
}
