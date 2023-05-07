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
	"math"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treydock/tsm_exporter/config"
)

var (
	mockedExitStatus = 0
	mockedStdout     string
	_, cancel        = context.WithTimeout(context.Background(), 5*time.Second)
)

func fakeExecCommand(ctx context.Context, command string, args ...string) *exec.Cmd {
	cs := []string{"-test.run=TestExecCommandHelper", "--", command}
	cs = append(cs, args...)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], cs...)
	es := strconv.Itoa(mockedExitStatus)
	cmd.Env = []string{"GO_WANT_HELPER_PROCESS=1",
		"STDOUT=" + mockedStdout,
		"EXIT_STATUS=" + es}
	return cmd
}

func TestExecCommandHelper(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}

	//nolint:staticcheck
	fmt.Fprintf(os.Stdout, os.Getenv("STDOUT"))
	i, _ := strconv.Atoi(os.Getenv("EXIT_STATUS"))
	os.Exit(i)
}

func setupGatherer(collector Collector) prometheus.Gatherer {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)
	gatherers := prometheus.Gatherers{registry}
	return gatherers
}

func TestDsmadmcQueryWithError(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 1
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := dsmadmcQuery(&config.Target{}, "query", ctx, log.NewNopLogger())
	if err == nil {
		t.Errorf("Expected error")
	}
}

func TestDsmadmcQueryWithNoResultsError(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 1
	mockedStdout = "ANR2034E SELECT: No match found using this criteria.\nANS8001I Return code 11.\n"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dsmadmcQuery(&config.Target{}, "query", ctx, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != "" {
		t.Errorf("Unexpected out: %s", out)
	}
}

func TestParseFloat(t *testing.T) {
	tests := []struct {
		Input         string
		Output        float64
		ExpectedError string
	}{
		{
			Input:         "99,5",
			Output:        99.5,
			ExpectedError: "",
		},
		{
			Input:         "13,52",
			Output:        13.52,
			ExpectedError: "",
		},
		{
			Input:         "",
			Output:        math.NaN(),
			ExpectedError: "",
		},
		{
			Input:         "1,500",
			Output:        0,
			ExpectedError: "strconv.ParseFloat: parsing \"1,500\": invalid syntax",
		},
	}
	for i, test := range tests {
		val, err := parseFloat(test.Input)
		if test.ExpectedError == "" {
			if err != nil {
				t.Errorf("Unexpected error in case %v: %v", i, err)
			}
		} else {
			if err == nil {
				t.Errorf("Expected error in case %v", i)
			} else if err.Error() != test.ExpectedError {
				t.Errorf("Unexpected error in case %v:\nExpected: %v\nGot: %v", i, test.ExpectedError, err.Error())
			}
		}
		if test.Input == "" {
			if !math.IsNaN(val) {
				t.Errorf("Unexpected value in case %v: Expected NaN\nGot: %v", i, val)
			}
		} else if val != test.Output {
			t.Errorf("Unexpected value in case %v:\nExpected: %v\nGot: %v", i, test.Output, val)
		}
	}
}

func TestParseTime(t *testing.T) {
	input := "2020-12-05 01:01:26.000000"
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	zone := "America/New_York"
	timezone = &zone
	if output, err := parseTime(input, &config.Target{}); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if output.Unix() != 1607148086 {
		t.Errorf("Unexpected value for parsed time, case 1")
	}
	if output, err := parseTime(input, &config.Target{Timezone: "America/Mexico_City"}); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if output.Unix() != 1607151686 {
		t.Errorf("Unexpected value for parsed time, case 1")
	}
}

func TestParseTimeError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	zone := "America/FOO"
	timezone = &zone
	input := "2020-12-05 01:01:26.000000"
	if _, err := parseTime(input, &config.Target{}); err == nil {
		t.Errorf("Expected error for case 1")
	}
	zone = "America/New_York"
	timezone = &zone
	input = ""
	if _, err := parseTime(input, &config.Target{}); err == nil {
		t.Errorf("Expected error for case 2")
	}
}
