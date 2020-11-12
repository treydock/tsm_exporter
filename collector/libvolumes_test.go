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
	mockLibVolumeStdout = `
LTO-5,Private,147
LTO-5,Scratch,342
LTO-6,Private,573
LTO-6,Scratch,365
LTO-7,Private,1082
LTO-7,Scratch,153
`
)

func TestLibVolumesParse(t *testing.T) {
	metrics := libvolumesParse(mockLibVolumeStdout, log.NewNopLogger())
	if len(metrics) != 6 {
		t.Errorf("Expected 6 metrics, got %v", len(metrics))
	}
}

func TestLibVolumesCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcLibVolumesExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockLibVolumeStdout, nil
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="libvolumes"} 0
	# HELP tsm_libvolume_media Number of tapes
	# TYPE tsm_libvolume_media gauge
	tsm_libvolume_media{mediatype="LTO-5",status="private"} 147
	tsm_libvolume_media{mediatype="LTO-5",status="scratch"} 342
	tsm_libvolume_media{mediatype="LTO-6",status="private"} 573
	tsm_libvolume_media{mediatype="LTO-6",status="scratch"} 365
	tsm_libvolume_media{mediatype="LTO-7",status="private"} 1082
	tsm_libvolume_media{mediatype="LTO-7",status="scratch"} 153
	`
	collector := NewLibVolumesExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 8 {
		t.Errorf("Unexpected collection count %d, expected 8", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_libvolume_media",
		"tsm_exporter_collect_error"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestLibVolumesCollectorError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcLibVolumesExec = func(target *config.Target, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	expected := `
    # HELP tsm_exporter_collect_error Indicates if error has occurred during collection
    # TYPE tsm_exporter_collect_error gauge
    tsm_exporter_collect_error{collector="libvolumes"} 1
	`
	collector := NewLibVolumesExporter(&config.Target{}, log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 2 {
		t.Errorf("Unexpected collection count %d, expected 2", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_libvolume_media",
		"tsm_exporter_collect_error"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

/*func TestDsmadmcLibVolumes(t *testing.T) {
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
}*/
