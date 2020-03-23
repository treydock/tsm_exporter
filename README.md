# TSM Prometheus exporter

[![Build Status](https://circleci.com/gh/treydock/tsm_exporter/tree/master.svg?style=shield)](https://circleci.com/gh/treydock/tsm_exporter)
[![GitHub release](https://img.shields.io/github/v/release/treydock/tsm_exporter?include_prereleases&sort=semver)](https://github.com/treydock/tsm_exporter/releases/latest)
![GitHub All Releases](https://img.shields.io/github/downloads/treydock/tsm_exporter/total)
[![codecov](https://codecov.io/gh/treydock/tsm_exporter/branch/master/graph/badge.svg)](https://codecov.io/gh/treydock/tsm_exporter)

# TSM Prometheus exporter

The TSM exporter collects metrics from the Tivoli Storage Manager (IBM Spectrum Protect).

This expecter is intended to query multiple TSM servers from an external host.

The `/tsm` metrics endpoint exposes TSM metrics and requires the `target` parameter.

The `/metrics` endpoint exposes Go and process metrics for this exporter.

## Collectors

Collectors are enabled or disabled via a config file.

Name | Description | Default
-----|-------------|--------
volumes | Collect count of unavailable or readonly volumes | Enabled
log | Collect active log space metrics | Enabled
db | Collect DB space information | Enabled
libvolumes | Collect count of scratch tapes | Enabled
drives | Collect count of offline drives | Enabled
events | Collect event duration and number of not completed events | Enabled
replicationview | Collect metrics about replication | Enabled

## Configuration

The configuration defines targets that are to be queried. Example:

```yaml
targets:
  tsm1.example.com:
    id: somwell
    password: secret
    library_name: TAPE
    schedules:
    - MYSQL
    replication_node_names:
    - TESTDB
  tsm2.example.com:
    id: somwell
    password: secret
    collectors:
    - volumes
    - log
    - db
```

This exporter could then be queried via one of these two commands below.  The `tsm2.example.com` target will only run the `volumes`, `log` and `db` collectors.

```
curl http://localhost:9310/tsm?target=tsm1.example.com
curl http://localhost:9310/tsm?target=tsm2.example.com
```

The key for each target should match the `servername` value for the entry in `dsm.sys`.  You may optionally add the `servername` key to override the servername used when executing `dsmadmc`.

The `libvolumes` and `drives` collectors can be limited to a specific library name via `library_name` config value, eg: `library_name: TAPE`.

The `events` collector can be limited to specific schedules via the `schedules` config value.

The `replicationview` collector can be limited to specific node names via the `replication_node_names` config vlaue.

## Dependencies

This exporter relies on the `dsmadmc` command. The host running the exporter is expected to have both the `dsmadmc` executable and `/opt/tivoli/tsm/client/ba/bin/dsm.sys`.

The hosts being queried by this exporter must exist in `/opt/tivoli/tsm/client/ba/bin/dsm.sys`.

## Install

Download the [latest release](https://github.com/treydock/tsm_exporter/releases)

Add the user that will run `tsm_exporter`

```
groupadd -r tsm_exporter
useradd -r -d /var/lib/tsm_exporter -s /sbin/nologin -M -g tsm_exporter -M tsm_exporter
```

Install compiled binaries after extracting tar.gz from release page.

```
cp /tmp/tsm_exporter /usr/local/bin/tsm_exporter
```

Install the necessary dependencies, see [dependencies section](#dependencies)

Add the necessary config, see [configuration section](#configuration)

Add systemd unit file and start service. Modify the `ExecStart` with desired flags.

```
cp systemd/tsm_exporter.service /etc/systemd/system/tsm_exporter.service
systemctl daemon-reload
systemctl start tsm_exporter
```

## Build from source

To produce the `tsm_exporter` binary:

```
make build
```

Or

```
go get github.com/treydock/tsm_exporter
```

## Prometheus configs

The following example assumes this exporter is running on the Prometheus server and communicating to the remote TSM hosts.

```yaml
- job_name: tsm
  metrics_path: /tsm
  static_configs:
  - targets:
    - tsm1.example.com
    - tsm2.example.com
  relabel_configs:
  - source_labels: [__address__]
    target_label: __param_target
  - source_labels: [__param_target]
    target_label: instance
  - target_label: __address__
    replacement: 127.0.0.1:9310
- job_name: tsm-metrics
  metrics_path: /metrics
  static_configs:
  - targets:
    - localhost:9310
```
