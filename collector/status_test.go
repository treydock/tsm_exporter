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
	mockStatusStdout = `
SP03,,1500,Off,Yes,05/22/2019 13:26:41,03/11/2020 14:36:50,On,90 Day(s),0,8,Closed,No,Enabled,,,Off,30 Day(s),3700264,116 M,30 Day(s),30 Day(s),03/12/2020 14:38:16,Valid,Active,150,75,10 Day(s),5 Day(s),25,Client,Client,Client,0 %,Any,CONSOLE ACTLOG SYSLOG,Off,60,,Off,120 Minute(s),c8.ee.08.6c.34.73.e9.11.8a.10.4c.d9.8f.3a.9c.76,Off,/tsm/db/tsminst1,"2,096,671.99","219,392.85","1,877,279.14",AES,180,Enabled,SP5,ALL_DATA,ALL_DATA,ALL_DATA,30 Day(s),,No,Local,,365 Day(s),On,614.64,0.00,03/18/2020 14:39:03,"507,029,699.15",4,03/22/2020 14:56:25,
`
)

func TestStatusParse(t *testing.T) {
	metrics := statusParse(mockStatusStdout, log.NewNopLogger())
	if metrics.status != 1 {
		t.Errorf("Expected status 1, got %v", metrics.status)
	}
	if metrics.reason != "" {
		t.Errorf("Expected no reason, got %v", metrics.reason)
	}
}

func TestStatusParseNoServername(t *testing.T) {
	metrics := statusParse("", log.NewNopLogger())
	if metrics.status != 0 {
		t.Errorf("Expected status 0, got %v", metrics.status)
	}
	if metrics.reason != "servername not found" {
		t.Errorf("Expected no servername reason, got %v", metrics.reason)
	}
}

func TestStatusCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcStatusExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockStatusStdout, nil
	}
	expected := `
    # HELP tsm_status Status of TSM, 1=online 0=failure
    # TYPE tsm_status gauge
    tsm_status{reason="",servername="SP03"} 1
	`
	collector := NewStatusExporter(&config.Target{}, log.NewNopLogger(), false)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 2 {
		t.Errorf("Unexpected collection count %d, expected 2", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_status",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestStatusCollectorError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcStatusExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	expected := `
    # HELP tsm_status Status of TSM, 1=online 0=failure
    # TYPE tsm_status gauge
    tsm_status{reason="error",servername=""} 0
	`
	collector := NewStatusExporter(&config.Target{}, log.NewNopLogger(), false)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 2 {
		t.Errorf("Unexpected collection count %d, expected 2", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_status",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestStatusCollectorTimeout(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcStatusExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", context.DeadlineExceeded
	}
	expected := `
    # HELP tsm_status Status of TSM, 1=online 0=failure
    # TYPE tsm_status gauge
    tsm_status{reason="timeout",servername=""} 0
	`
	collector := NewStatusExporter(&config.Target{}, log.NewNopLogger(), false)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 2 {
		t.Errorf("Unexpected collection count %d, expected 2", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"tsm_status",
		"tsm_exporter_collect_error", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestStatusCollectorCache(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	DsmadmcStatusExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockStatusStdout, nil
	}
	errorMetric := `
    # HELP tsm_status Status of TSM, 1=online 0=failure
    # TYPE tsm_status gauge
    tsm_status{reason="error",servername="SP03"} 0
	`
	timeoutMetric := `
    # HELP tsm_status Status of TSM, 1=online 0=failure
    # TYPE tsm_status gauge
    tsm_status{reason="timeout",servername="SP03"} 0
	`
	collector := NewStatusExporter(&config.Target{}, log.NewNopLogger(), true)
	gatherers := setupGatherer(collector)
	if val := testutil.CollectAndCount(collector); val != 2 {
		t.Errorf("Unexpected collection count %d, expected 2", val)
	}
	DsmadmcStatusExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", fmt.Errorf("Error")
	}
	if val := testutil.CollectAndCount(collector); val != 2 {
		t.Errorf("Unexpected collection count %d, expected 2", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(errorMetric),
		"tsm_status", "tsm_exporter_collect_error"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
	DsmadmcStatusExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "", context.DeadlineExceeded
	}
	if val := testutil.CollectAndCount(collector); val != 2 {
		t.Errorf("Unexpected collection count %d, expected 2", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(timeoutMetric),
		"tsm_status", "tsm_exporter_collect_timeout"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestDsmadmcStatus(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 0
	mockedStdout = "foo"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dsmadmcStatus(&config.Target{}, ctx, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != mockedStdout {
		t.Errorf("Unexpected out: %s", out)
	}
}
