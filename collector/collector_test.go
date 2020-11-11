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
	"io/ioutil"
	"os"
	"testing"
	//"time"

	"github.com/go-kit/kit/log"
	//"github.com/google/goexpect"
	"github.com/prometheus/client_golang/prometheus"
	//"github.com/treydock/tsm_exporter/config"
	//"gopkg.in/alecthomas/kingpin.v2"
)

// TODO: Make these tests work
/*func TestDsmadmcQuery(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{"--collector.dsmadmc.debug"}); err != nil {
		t.Fatal(err)
	}
	output := []expect.Batcher{
		&expect.BSnd{`Enter your password:`},
		&expect.BSnd{`

Protect: SP01>`},
		&expect.BSnd{`
Protect: SP01>`},
	}
	exp, _, err := expect.SpawnFake(output, 2*time.Second)
	defer exp.Close()
	spawnExpectFunc = func(cmd string, timeout time.Duration) (*expect.GExpect, error) {
		return exp, err
	}
	queryOutput = func(path string, query string, logger log.Logger) (string, error) {
		return "some output", nil
	}
	w := log.NewSyncWriter(os.Stderr)
	logger := log.NewLogfmtLogger(w)
	out, err := dsmadmcQuery(&config.Target{}, "query", 2, logger)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if out != "some output" {
		t.Errorf("Unexpected output: %s", out)
	}
}

func TestDsmadmcQueryError(t *testing.T) {
	spawnExpectFunc = func(cmd string, timeout time.Duration) (*expect.GExpect, error) {
		output := []expect.Batcher{
			&expect.BSnd{`
Failed to connect`},
		}
		exp, _, err := expect.SpawnFake(output, 2*time.Second)
		return exp, err
	}
	queryOutput = func(path string, query string, logger log.Logger) (string, error) {
		return "some output", nil
	}
	out, err := dsmadmcQuery(&config.Target{}, "query", 2, log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if out != "some output" {
		t.Errorf("Unexpected output. %s", out)
	}
}*/

func TestGetQueryOutput(t *testing.T) {
	tmp, _ := ioutil.TempFile("", "tsm_exporter_test")
	defer os.Remove(tmp.Name())
	data := []byte(`query
returned,data
quit
`)
	if _, err := tmp.Write(data); err != nil {
		t.Errorf("Unexpected error writing to tmp file: %v", err)
	}
	out, err := getQueryOutput(tmp.Name(), "query", log.NewNopLogger())
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if out != "returned,data" {
		t.Errorf("Unexpected output: %s", out)
	}
}

func setupGatherer(collector Collector) prometheus.Gatherer {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)
	gatherers := prometheus.Gatherers{registry}
	return gatherers
}
