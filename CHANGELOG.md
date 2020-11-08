## 0.7.0 / TBD

### BREAKING CHANGES

* Remove --exporter.use-cache flag and all caching logic
* For drive metrics, replace name label with drive
* For storage_pool metrics, replace name label with storagepool
* For volume metrics, replace name label with volume
* Improve events collector to not require saving any data in memory, remove --collector.events.duration-cache flag
* Improve replicationview collector to not store any data in memory, remove --collector.replicationview.metric-cache flag

## 0.6.0 / 2020-11-06

* Update to Go 1.15 and update dependencies
* Add tsm_volume_utilized_percent and tsm_volume_estimated_capacity_bytes metrics
* Add volumeusage collector
* Add occupancy collector
* Add stgpools collector

## 0.5.0 / 2020-05-28

* BREAKING: Rename tsm_tapes_scratch to tsm_libvolume_scratch
* Add tsm_libvolume_media metrics
* Add tsm_drive_state_info and tsm_drive_volume_info metrics
* Add tsm_db_last_backup_time metric

## 0.4.0 / 2020-04-04

* Simplified timeout and error handling

## 0.3.0 / 2020-03-25

* Refactor drives collector to expose a metric per drive

## 0.2.2 / 2020-03-25

* Fix issue where replication endtime and duration would be negative

## 0.2.1 / 2020-03-25

* Ensure multiple non completed replications are counted

## 0.2.0 / 2020-03-23

* Ensure DSM_LOG can be set via command line argument

## 0.1.0 / 2020-03-23

* Add status collector

## 0.0.1 / 2020-03-23

* Initial Release

