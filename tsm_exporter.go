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
	"log/slog"
	"net/http"
	"os"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/common/promslog/flag"
	"github.com/prometheus/common/version"
	"github.com/treydock/tsm_exporter/collector"
	"github.com/treydock/tsm_exporter/config"
)

const (
	tsmEndpoint     = "/tsm"
	metricsEndpoint = "/metrics"
)

var (
	configFile    = kingpin.Flag("config.file", "Path to exporter config file").Default("tsm_exporter.yaml").String()
	listenAddress = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9310").String()
)

func metricsHandler(config *config.Config, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		registry := prometheus.NewRegistry()

		t := r.URL.Query().Get("target")
		if t == "" {
			http.Error(w, "'target' parameter must be specified", http.StatusBadRequest)
			return
		}
		target, ok := config.Targets[t]
		if !ok {
			http.Error(w, fmt.Sprintf("Unknown target %s", t), http.StatusNotFound)
			return
		}

		target.Lock()
		tsmCollector := collector.NewCollector(target, logger)
		defer target.Unlock()
		for key, collector := range tsmCollector.Collectors {
			logger.Debug(fmt.Sprintf("Enabled collector %s", key))
			registry.MustRegister(collector)
		}

		gatherers := prometheus.Gatherers{registry}

		// Delegate http serving to Prometheus client library, which will call collector.Collect.
		h := promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{})
		h.ServeHTTP(w, r)
	}
}

func run(logger *slog.Logger) {
	logger.Info("Starting tsm_exporter", "version", version.Info())
	logger.Info("Build context", "build_context", version.BuildContext())
	logger.Info("Starting Server", "address", *listenAddress)

	sc := &config.SafeConfig{}

	if err := sc.ReloadConfig(*configFile); err != nil {
		logger.Error("Error loading config", "err", err)
		os.Exit(1)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		//nolint:errcheck
		w.Write([]byte(`<html>
             <head><title>TSM Exporter</title></head>
             <body>
             <h1>TSM Exporter</h1>
             <p><a href='` + tsmEndpoint + `'>TSM Metrics</a></p>
             <p><a href='` + metricsEndpoint + `'>Exporter Metrics</a></p>
             </body>
             </html>`))
	})
	http.Handle(tsmEndpoint, metricsHandler(sc.C, logger))
	http.Handle(metricsEndpoint, promhttp.Handler())
	err := http.ListenAndServe(*listenAddress, nil)
	if err != nil {
		logger.Error("Server error", "err", err)
		os.Exit(1)
	}
}

func main() {
	promslogConfig := &promslog.Config{}
	flag.AddFlags(kingpin.CommandLine, promslogConfig)
	kingpin.Version(version.Print("tsm_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	logger := promslog.New(promslogConfig)

	run(logger)
}
