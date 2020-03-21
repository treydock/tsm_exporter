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

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/treydock/tsm_exporter/collector"
	"github.com/treydock/tsm_exporter/config"
)

const (
	address = "localhost:19310"
)

var (
	mockedDBStdout  = "88.6,TSMDB1,3092796,1453663,98.3,0,11607707032,28836868,2096672,28836092,642976,25743296\n"
	mockedLogStdout = "32426.00,32768.00,342.00\n"
)

func TestMain(m *testing.M) {
	target1 := config.Target{}
	target2 := config.Target{Collectors: []string{"volumes"}}
	c := &config.Config{}
	c.Targets = make(map[string]*config.Target)
	c.Targets["test1"] = &target1
	c.Targets["test2"] = &target2
	go func() {
		http.Handle("/tsm", metricsHandler(c, log.NewNopLogger()))
		err := http.ListenAndServe(address, nil)
		if err != nil {
			os.Exit(1)
		}
	}()
	time.Sleep(1 * time.Second)

	exitVal := m.Run()

	os.Exit(exitVal)
}

func TestMetricsHandler(t *testing.T) {
	collector.DsmadmcVolumesUnavailExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "0\n", nil
	}
	collector.DsmadmcDBExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockedDBStdout, nil
	}
	collector.DsmadmcLogExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockedLogStdout, nil
	}
	body, err := queryExporter("target=test1", http.StatusOK)
	if err != nil {
		t.Fatalf("Unexpected error GET /tsm: %s", err.Error())
	}
	if !strings.Contains(body, "tsm_exporter_collect_error{collector=\"volumes\"} 0") {
		t.Errorf("Unexpected value for tsm_exporter_collect_error")
	}
}

func TestMetricsHandlerCollectorsDefined(t *testing.T) {
	collector.DsmadmcVolumesUnavailExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return "0\n", nil
	}
	collector.DsmadmcDBExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockedDBStdout, nil
	}
	collector.DsmadmcLogExec = func(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
		return mockedLogStdout, nil
	}
	body, err := queryExporter("target=test2", http.StatusOK)
	if err != nil {
		t.Fatalf("Unexpected error GET /tsm: %s", err.Error())
	}
	if !strings.Contains(body, "tsm_exporter_collect_error{collector=\"volumes\"} 0") {
		t.Errorf("Unexpected value for tsm_exporter_collect_error")
	}
	if strings.Contains(body, "tsm_db_space_total_bytes") {
		t.Errorf("Should not contain tsm_db_space_total_bytes metric")
	}
}

func TestMetricsHandlerNoTarget(t *testing.T) {
	_, _ = queryExporter("", http.StatusBadRequest)
}

func TestMetricsHandlerBadTarget(t *testing.T) {
	_, _ = queryExporter("target=dne", http.StatusNotFound)
}

func queryExporter(param string, want int) (string, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/tsm?%s", address, param))
	if err != nil {
		return "", err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if err := resp.Body.Close(); err != nil {
		return "", err
	}
	if have := resp.StatusCode; want != have {
		return "", fmt.Errorf("want /metrics status code %d, have %d. Body:\n%s", want, have, b)
	}
	return string(b), nil
}
