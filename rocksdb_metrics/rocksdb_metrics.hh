//
// Created by zhengcf on 2019-05-25.
//

#pragma once

#include <iostream>
#include <rocksdb/db.h>
#include <rocksdb/statistics.h>
#include <rocksdb/listener.h>
#include "metrics.hh"

class RocksdbStatistics;

class StatisticsEventListener : public rocksdb::EventListener {
public:
    StatisticsEventListener(const std::string &db_name, RocksdbStatistics &statistics)
            : db_name_(db_name), statistics_(statistics) {}

    void OnFlushCompleted(rocksdb::DB *db, const rocksdb::FlushJobInfo &info) override;

    void OnCompactionCompleted(rocksdb::DB *db,
                               const rocksdb::CompactionJobInfo &info) override;

    void OnExternalFileIngested(rocksdb::DB *db, const rocksdb::ExternalFileIngestionInfo &info) override;

    void OnStallConditionsChanged(const rocksdb::WriteStallInfo &info) override;

private:
    const char *GetCompactionReasonString(rocksdb::CompactionReason compaction_reason);

    const char *GetWriteStallConditionString(rocksdb::WriteStallCondition c);

    std::string db_name_;
    RocksdbStatistics &statistics_;
};

class RocksdbStatistics : public BaseMetrics {
public:
    RocksdbStatistics();

    void
    FlushMetrics(rocksdb::DB &db, const std::string &name,
                 const std::vector<rocksdb::ColumnFamilyHandle *> &db_cfs);

private:
    void FlushEngineTickerMetrics(rocksdb::Tickers t, const uint64_t value, const std::string &name);

    void
    FlushEngineHistogramMetrics(rocksdb::Histograms t, const rocksdb::HistogramData &value,
                                const std::string &name);

    void FlushEngineProperties(rocksdb::DB &db, const std::string &name,
                               const std::vector<rocksdb::ColumnFamilyHandle *> &db_cf);


public:

#define _make_counter_family(val) \
    val(STORE_ENGINE_CACHE_EFFICIENCY_VEC,      "engine_cache_efficiency",      "Efficiency of rocksdb's block cache",                  "db", "type") \
    val(STORE_ENGINE_MEMTABLE_EFFICIENCY_VEC,   "engine_memtable_efficiency",   "Hit and miss of memtable",                             "db", "type") \
    val(STORE_ENGINE_GET_SERVED_VEC,            "engine_get_served",            "Get queries served by engine",                         "db", "type") \
    val(STORE_ENGINE_WRITE_SERVED_VEC,          "engine_write_served",          "Write queries served by engine",                       "db", "type") \
    val(STORE_ENGINE_BLOOM_EFFICIENCY_VEC,      "engine_bloom_efficiency",      "Efficiency of rocksdb's bloom filter",                 "db", "type") \
    val(STORE_ENGINE_FLOW_VEC,                  "engine_flow_bytes",            "Bytes and keys of read/written",                       "db", "type") \
    val(STORE_ENGINE_STALL_MICROS,              "engine_stall_micro_seconds",   "Stall micros",                                         "db")         \
    val(STORE_ENGINE_COMPACTION_FLOW_VEC,       "engine_compaction_flow_bytes", "Bytes of read/written during compaction",              "db", "type") \
    val(STORE_ENGINE_COMPACTION_DROP_VEC,       "engine_compaction_key_drop",   "Count the reasons for key drop during compaction",     "db", "type") \
    val(STORE_ENGINE_COMPACTION_NUM_CORRUPT_KEYS_VEC, "engine_compaction_num_corrupt_keys", "Number of corrupt keys during compaction", "db", "cf")   \
    val(STORE_ENGINE_COMPACTION_REASON_VEC,     "engine_compaction_reason",     "Number of compaction reason",                          "db", "cf", "reason") \
    val(STORE_ENGINE_LOCATE_VEC,                "engine_locate",                "Number of calls to seek/next/prev",                    "db", "type") \
    val(STORE_ENGINE_FILE_STATUS_VEC,           "engine_file_status",           "Number of different status of files",                  "db", "type") \
    val(STORE_ENGINE_READ_AMP_FLOW_VEC,         "engine_read_amp_flow_bytes",   "Bytes of read amplification",                          "db", "type") \
    val(STORE_ENGINE_NO_ITERATORS,              "engine_no_iterator",           "Number of iterators currently open",                   "db")         \
    val(STORE_ENGINE_WAL_FILE_SYNCED,           "engine_wal_file_synced",       "Number of times WAL sync is done",                     "db")         \
    val(STORE_ENGINE_EVENT_COUNTER_VEC,         "engine_event_total",           "Number of engine events",                              "db", "cf", "type")


#define _make_gauge_family(val) \
    val(STORE_ENGINE_SIZE_GAUGE_VEC,                "engine_size_bytes",                "Sizes of each column families",                "db", "type") \
    val(STORE_ENGINE_BLOCK_CACHE_USAGE_GAUGE_VEC,   "engine_block_cache_size_bytes",    "Usage of each column families' block cache",   "db", "cf")   \
    val(STORE_ENGINE_MEMORY_GAUGE_VEC,              "engine_memory_bytes",              "Sizes of each column families",                "db", "cf", "type")   \
    val(STORE_ENGINE_ESTIMATE_NUM_KEYS_VEC,         "engine_estimate_num_keys",         "Estimate num keys of each column families",    "db", "cf")           \
    val(STORE_ENGINE_GET_MICROS_VEC,                "engine_get_micro_seconds",         "Histogram of get micros",                      "db", "type")         \
    val(STORE_ENGINE_WRITE_MICROS_VEC,              "engine_write_micro_seconds",       "Histogram of write micros",                    "db", "type")         \
    val(STORE_ENGINE_COMPACTION_TIME_VEC,           "engine_compaction_time",           "Histogram of compaction time",                 "db", "type")         \
    val(STORE_ENGINE_TABLE_SYNC_MICROS_VEC,         "engine_table_sync_micro_seconds",  "Histogram of table sync micros",               "db", "type")         \
    val(STORE_ENGINE_COMPACTION_OUTFILE_SYNC_MICROS_VEC,    "engine_compaction_outfile_sync_micro_seconds",     "Histogram of compaction outfile sync micros",  "db", "type") \
    val(STORE_ENGINE_MANIFEST_FILE_SYNC_MICROS_VEC, "engine_manifest_file_sync_micro_seconds",                  "Histogram of manifest file sync micros",       "db", "type") \
    val(STORE_ENGINE_WAL_FILE_SYNC_MICROS_VEC,      "engine_wal_file_sync_micro_seconds",                       "Histogram of WAL file sync micros",            "db", "type") \
    val(STORE_ENGINE_STALL_L0_SLOWDOWN_COUNT_VEC,   "engine_stall_l0_slowdown_count",                           "Histogram of stall l0 slowdown count",         "db", "type") \
    val(STORE_ENGINE_STALL_MEMTABLE_COMPACTION_COUNT_VEC,   "engine_stall_memtable_compaction_count",           "Histogram of stall memtable compaction count", "db", "type") \
    val(STORE_ENGINE_STALL_LO_NUM_FILES_COUNT_VEC,          "engine_stall_l0_num_files_count",                  "Histogram of stall l0 num files count",        "db", "type") \
    val(STORE_ENGINE_HARD_RATE_LIMIT_DELAY_COUNT_VEC,       "engine_hard_rate_limit_delay_count",               "Histogram of hard rate limit delay count",     "db", "type") \
    val(STORE_ENGINE_SOFT_RATE_LIMIT_DELAY_COUNT_VEC,       "engine_soft_rate_limit_delay_count",               "Histogram of soft rate limit delay count",     "db", "type") \
    val(STORE_ENGINE_NUM_FILES_IN_SINGLE_COMPACTION_VEC,    "engine_num_files_in_single_compaction",            "Histogram of number of files in single compaction", "db", "type") \
    val(STORE_ENGINE_SEEK_MICROS_VEC,               "engine_seek_micro_seconds",        "Histogram of seek micros",                     "db", "type") \
    val(STORE_ENGINE_WRITE_STALL_VEC,               "engine_write_stall",               "Histogram of write stall",                     "db", "type") \
    val(STORE_ENGINE_SST_READ_MICROS_VEC,           "engine_sst_read_micros",           "Histogram of SST read micros",                 "db", "type") \
    val(STORE_ENGINE_NUM_SUBCOMPACTION_SCHEDULED_VEC,       "engine_num_subcompaction_scheduled",               "Histogram of number of subcompaction scheduled", "db", "type") \
    val(STORE_ENGINE_BYTES_PER_READ_VEC,            "engine_bytes_per_read",            "Histogram of bytes per read",                  "db", "type") \
    val(STORE_ENGINE_BYTES_PER_WRITE_VEC,           "engine_bytes_per_write",           "Histogram of bytes per write",                 "db", "type") \
    val(STORE_ENGINE_BYTES_COMPRESSED_VEC,          "engine_bytes_compressed",          "Histogram of bytes compressed",                "db", "type") \
    val(STORE_ENGINE_BYTES_DECOMPRESSED_VEC,        "engine_bytes_decompressed",        "Histogram of bytes decompressed",              "db", "type") \
    val(STORE_ENGINE_COMPRESSION_TIMES_NANOS_VEC,   "engine_compression_time_nanos",    "Histogram of compression time nanos",          "db", "type") \
    val(STORE_ENGINE_DECOMPRESSION_TIMES_NANOS_VEC, "engine_decompression_time_nanos",  "Histogram of decompression time nanos",        "db", "type") \
    val(STORE_ENGINE_PENDING_COMACTION_BYTES_VEC,   "engine_pending_compaction_bytes",  "Pending compaction bytes",                     "db", "cf")   \
    val(STORE_ENGINE_COMPRESSION_RATIO_VEC,         "engine_compression_ratio",         "Compression ratio at different levels",        "db", "cf", "level") \
    val(STORE_ENGINE_NUM_SNAPSHOTS_GAUGE_VEC,       "engine_num_snapshots",             "Number of unreleased snapshots",               "db") \
    val(STORE_ENGINE_OLDEST_SNAPSHOT_DURATION_GAUGE_VEC,    "engine_oldest_snapshot_duration",                  "Oldest unreleased snapshot duration in seconds", "db") \
    val(STORE_ENGINE_NUM_FILES_AT_LEVEL_VEC,        "engine_num_files_at_level",        "Number of files at each level",                "db", "cf", "level")      \
    val(STORE_ENGINE_NUM_IMMUTABLE_MEM_TABLE_VEC,   "engine_num_immutable_mem_table",   "Number of immutable mem-table",                "db", "cf")               \
    val(STORE_ENGINE_STALL_CONDITIONS_CHANGED_VEC,  "engine_stall_conditions_changed",  "Stall conditions changed of each column family", "db", "cf", "type")


#define _make_histogram_family(val) \
    val(STORE_ENGINE_COMPACTION_DURATIONS_VEC,  "engine_compaction_duration_seconds",   "Histogram of compaction duration seconds", {"db", "cf"}, prometheus::Histogram::ExponentialBuckets(0.005, 2.0, 20))

private:
    friend StatisticsEventListener;
    std::map<rocksdb::Tickers, const std::string> tickers_names_;
    std::map<rocksdb::Histograms, const std::string> histograms_names_;

#define _make_counter_params(param, name, help, label, ...)   \
    prometheus::Family<prometheus::Counter>& param;
    _make_counter_family(_make_counter_params)
#define _make_gauge_params(param, name, help, label, ...)   \
    prometheus::Family<prometheus::Gauge>& param;
    _make_gauge_family(_make_gauge_params)

#define _make_histogram_familys_param(param, name, help, label, ...)   \
    prometheus::Family<prometheus::Histogram>& param;
    _make_histogram_family(_make_histogram_familys_param)

    int dummy_;
};

