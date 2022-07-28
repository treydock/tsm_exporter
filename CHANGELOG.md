## 2.0.0-rc.0 / 2022-07-28

### Changes

* Replication refactor (#45)
  * Add metric tsm_replication_incomplete_start_timestamp_seconds
  * Add metric tsm_replication_incomplete_replicated_files
  * Remove metric tsm_replication_not_completed

## 1.2.2 / 2022-03-30

### Changes

* Do not error if LAST_BACKUP_DATE is empty (#44)

## 1.2.1 / 2022-03-08

### Changes

* Avoid duplicates with volumeusage query (#41)

## 1.2.0 / 2022-03-08

### Changes

* Update Go to 1.17
* Update Go module dependencies

## 1.1.0 / 2021-04-23

### Changes

* Update to Go 1.16
* Update Go module dependencies

## 1.0.0 / 2021-04-23

### **Breaking Changes**

* Remove --exporter.use-cache flag and all caching logic
* For drive metrics, replace name label with drive
* For storage_pool metrics, replace name label with storagepool
* For volume metrics, replace name label with volume
* Improve events collector to not require saving any data in memory, remove --collector.events.duration-cache flag
* Improve replicationview collector to not store any data in memory, remove --collector.replicationview.metric-cache flag
* Remove tsm_libvolume_scratch metric, use sum(tsm_libvolume_media{status="scratch"}) instead
* Make percent metrics into ratios
  * Rename tsm_storage_pool_utilized_percent to tsm_storage_pool_utilized_ratio
  * Rename tsm_volume_utilized_percent to tsm_volume_utilized_ratio
* Remove reason and servername label from tsm_status
* Make tsm_db_buffer_hit_ratio and tsm_db_pkg_hit_ratio a ratio between 0.0-1.0
* Rename tsm_db_buffer_total_requests to tsm_db_buffer_requests_total
* Rename tsm_db_last_backup_time to tsm_db_last_backup_timestamp_seconds
* Rename tsm_replication_end_time to tsm_replication_end_timestamp_seconds
* Rename tsm_replication_start_time to tsm_replication_start_timestamp_seconds

### Changes

* Add Docker container
* Fix libvolume query
* Add `library` label to `tsm_libvolume_media` metric
* Fix parsing to handle cases where TSM queries return decimal numbers with a comma instead of a period
* Use CSV parser when parsing TSM data from queries
* Improved error handling, return error metric if any parsing has errors
* If numeric columns in queries are empty, do not produce errors or metrics for that missing column
* Produce a metric for each possible drive state with `tsm_drive_state_info`
* Improved logging when parse errors are encountered
* Ensure `tsm_libvolume_media{status="private"}` and `tsm_libvolume_media{status="scratch"}` are always present for each mediatype/library combination
* Add `tsm_occupancy_reporting_bytes` metric
* Add metrics for `stgpools` collector
  * `tsm_storage_pool_cloud_total_bytes`
  * `tsm_storage_pool_cloud_used_bytes`
  * `tsm_storage_pool_estimated_capacity_bytes`
  * `tsm_storage_pool_local_estimated_capacity_bytes`
  * `tsm_storage_pool_local_logical_ratio`
  * `tsm_storage_pool_local_utilized_ratio`
  * `tsm_storage_pool_logical_ratio`
* Add `summary` collector
* Fix time parsing to be timezone aware, add options to adjust timezone
* Add metrics to `events` collector
  * `tsm_schedule_start_timestamp_seconds`
  * `tsm_schedule_completed_timestamp_seconds`

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

