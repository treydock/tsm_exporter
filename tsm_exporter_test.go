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
	mockStatusStdout = `
SP03,,1500,Off,Yes,05/22/2019 13:26:41,03/11/2020 14:36:50,On,90 Day(s),0,8,Closed,No,Enabled,,,Off,30 Day(s),3700264,116 M,30 Day(s),30 Day(s),03/12/2020 14:38:16,Valid,Active,150,75,10 Day(s),5 Day(s),25,Client,Client,Client,0 %,Any,CONSOLE ACTLOG SYSLOG,Off,60,,Off,120 Minute(s),c8.ee.08.6c.34.73.e9.11.8a.10.4c.d9.8f.3a.9c.76,Off,/tsm/db/tsminst1,"2,096,671.99","219,392.85","1,877,279.14",AES,180,Enabled,SP5,ALL_DATA,ALL_DATA,ALL_DATA,30 Day(s),,No,Local,,365 Day(s),On,614.64,0.00,03/18/2020 14:39:03,"507,029,699.15",4,03/22/2020 14:56:25,
`
	mockVolumeStdout = `
UNAVAILABLE
UNAVAILABLE
READONLY
`
	mockedDBStdout      = "88.6,TSMDB1,3092796,1453663,2020-05-22 08:10:00.000000,98.3,0,11607707032,28836868,2096672,28836092,642976,25743296\n"
	mockedLogStdout     = "32426.00,32768.00,342.00\n"
	mockLibVolumeStdout = `
LTO-5,Private,147
LTO-5,Scratch,342
LTO-6,Private,573
LTO-6,Scratch,365
LTO-7,Private,1082
LTO-7,Scratch,153
`
	mockDriveStdout = `
LIB1,TAPE10,YES,LOADED,FOO1
LIB1,TAPE11,YES,LOADED,FOO2
LIBENC,TAPE00,YES,EMPTY,
LIBENC,TAPE01,NO,EMPTY,
`
	mockEventCompletedStdout = `
FOO,Completed,2020-03-22 05:09:43.000000,2020-03-22 05:41:14.000000
`
	mockEventNotCompletedStdout = `
FOO,Future
BAR,Not Started
BAR,Not Started
`
	mockReplicationViewCompletedStdout = `
TEST2DB2,/TEST2CONF,2020-03-23 00:45:29.000000,2020-03-23 06:06:45.000000,2,167543418
TEST2DB2,/TEST4,2020-03-23 00:45:29.000000,2020-03-23 06:06:45.000000,2,1052637876956
`
	mockReplicationViewNotCompletedStdout = `
FOO,/BAR
BAR,/BAZ
`
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
	collector.DsmadmcStatusExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockStatusStdout, nil
	}
	collector.DsmadmcVolumesExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockVolumeStdout, nil
	}
	collector.DsmadmcDBExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockedDBStdout, nil
	}
	collector.DsmadmcLogExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockedLogStdout, nil
	}
	collector.DsmadmcLibVolumesExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockLibVolumeStdout, nil
	}
	collector.DsmadmcDrivesExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockDriveStdout, nil
	}
	collector.DsmadmcEventsCompletedExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockEventCompletedStdout, nil
	}
	collector.DsmadmcEventsNotCompletedExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockEventNotCompletedStdout, nil
	}
	collector.DsmadmcReplicationViewsCompletedExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockReplicationViewCompletedStdout, nil
	}
	collector.DsmadmcReplicationViewsNotCompletedExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockReplicationViewNotCompletedStdout, nil
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
	collector.DsmadmcStatusExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockStatusStdout, nil
	}
	collector.DsmadmcVolumesExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockVolumeStdout, nil
	}
	collector.DsmadmcDBExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockedDBStdout, nil
	}
	collector.DsmadmcLogExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockedLogStdout, nil
	}
	collector.DsmadmcLibVolumesExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockLibVolumeStdout, nil
	}
	collector.DsmadmcDrivesExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockDriveStdout, nil
	}
	collector.DsmadmcEventsCompletedExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockEventCompletedStdout, nil
	}
	collector.DsmadmcEventsNotCompletedExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockEventNotCompletedStdout, nil
	}
	collector.DsmadmcReplicationViewsCompletedExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockReplicationViewCompletedStdout, nil
	}
	collector.DsmadmcReplicationViewsNotCompletedExec = func(target *config.Target, logger log.Logger) (string, error) {
		return mockReplicationViewNotCompletedStdout, nil
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
