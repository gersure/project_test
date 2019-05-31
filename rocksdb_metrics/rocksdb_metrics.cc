//
// Created by zhengcf on 2019-05-25.
//

#include "rocksdb_metrics.hh"


void StatisticsEventListener::OnFlushCompleted(rocksdb::DB *db, const rocksdb::FlushJobInfo &info) {
    statistics_.STORE_ENGINE_EVENT_COUNTER_VEC
            .WithLabelValues({db_name_, info.cf_name, "flush"})
            .Increment();
    statistics_.STORE_ENGINE_STALL_CONDITIONS_CHANGED_VEC
            .WithLabelValues({db_name_, info.cf_name, "triggered_writes_slowdown"})
            .Set(int64_t(info.triggered_writes_slowdown));
    statistics_.STORE_ENGINE_STALL_CONDITIONS_CHANGED_VEC
            .WithLabelValues({db_name_, info.cf_name, "triggered_writes_stop"})
            .Set(int64_t(info.triggered_writes_stop));
}

void StatisticsEventListener::OnCompactionCompleted(rocksdb::DB *db,
                                                    const rocksdb::CompactionJobInfo &info) {
    statistics_.STORE_ENGINE_EVENT_COUNTER_VEC
            .WithLabelValues({db_name_, info.cf_name, "compaction"})
            .Increment();
    statistics_.STORE_ENGINE_COMPACTION_DURATIONS_VEC
            .WithLabelValues({db_name_, info.cf_name})
            .Observe(double(info.stats.elapsed_micros) / 1000000.0);
    statistics_.STORE_ENGINE_COMPACTION_NUM_CORRUPT_KEYS_VEC
            .WithLabelValues({db_name_, info.cf_name})
            .Increment(info.stats.num_corrupt_keys);
    statistics_.STORE_ENGINE_COMPACTION_REASON_VEC
            .WithLabelValues({db_name_,
                              info.cf_name,
                              GetCompactionReasonString(info.compaction_reason)})
            .Increment();
}

void StatisticsEventListener::OnExternalFileIngested(rocksdb::DB *db,
                                                     const rocksdb::ExternalFileIngestionInfo &info) {
    statistics_.STORE_ENGINE_EVENT_COUNTER_VEC
            .WithLabelValues({db_name_, info.cf_name, "ingestion"})
            .Increment();
}

void StatisticsEventListener::OnStallConditionsChanged(const rocksdb::WriteStallInfo &info) {
    statistics_.STORE_ENGINE_EVENT_COUNTER_VEC
            .WithLabelValues({db_name_, info.cf_name, "stall_conditions_changed"})
            .Increment();

    statistics_.STORE_ENGINE_STALL_CONDITIONS_CHANGED_VEC
            .WithLabelValues({db_name_,
                              info.cf_name,
                              GetWriteStallConditionString(info.condition.cur),
                             })
            .Set(1);
    statistics_.STORE_ENGINE_STALL_CONDITIONS_CHANGED_VEC
            .WithLabelValues({db_name_,
                              info.cf_name,
                              GetWriteStallConditionString(info.condition.prev)})
            .Set(0);
}

const char *StatisticsEventListener::GetCompactionReasonString(rocksdb::CompactionReason compaction_reason) {
    using namespace rocksdb;
    switch (compaction_reason) {
        case CompactionReason::kUnknown:
            return "Unknown";
        case CompactionReason::kLevelL0FilesNum:
            return "LevelL0FilesNum";
        case CompactionReason::kLevelMaxLevelSize:
            return "LevelMaxLevelSize";
        case CompactionReason::kUniversalSizeAmplification:
            return "UniversalSizeAmplification";
        case CompactionReason::kUniversalSizeRatio:
            return "UniversalSizeRatio";
        case CompactionReason::kUniversalSortedRunNum:
            return "UniversalSortedRunNum";
        case CompactionReason::kFIFOMaxSize:
            return "FIFOMaxSize";
        case CompactionReason::kFIFOReduceNumFiles:
            return "FIFOReduceNumFiles";
        case CompactionReason::kFIFOTtl:
            return "FIFOTtl";
        case CompactionReason::kManualCompaction:
            return "ManualCompaction";
        case CompactionReason::kFilesMarkedForCompaction:
            return "FilesMarkedForCompaction";
        case CompactionReason::kBottommostFiles:
            return "BottommostFiles";
        case CompactionReason::kTtl:
            return "Ttl";
        case CompactionReason::kFlush:
            return "Flush";
        case CompactionReason::kExternalSstIngestion:
            return "ExternalSstIngestion";
        case CompactionReason::kNumOfReasons:
            // fall through
        default:
            return "Invalid";
    }
}

const char *StatisticsEventListener::GetWriteStallConditionString(rocksdb::WriteStallCondition c) {
    using namespace rocksdb;
    switch (c) {
        case WriteStallCondition::kNormal :
            return "normal";
        case WriteStallCondition::kDelayed :
            return "delayed";
        case WriteStallCondition::kStopped :
            return "stopped";
        default:
            return "Invalid";
    }
}


RocksdbStatistics::RocksdbStatistics()
        : BaseMetrics(),
#define _init_counter_familys(param, name, help, label, ...)   \
    param(prometheus::BuildCounter() \
    .Name(name) \
    .Help(help) \
    .Register(*registry_) \
    .LabelNames({label, __VA_ARGS__})),
        _make_counter_family(_init_counter_familys)

#define _init_gauge_familys(param, name, help, label, ...)   \
    param(prometheus::BuildGauge() \
    .Name(name) \
    .Help(help) \
    .Register(*registry_) \
    .LabelNames({label, __VA_ARGS__})),
        _make_gauge_family(_init_gauge_familys)

#define _init_histogram_familys(param, name, help, label, ...)   \
    param(prometheus::BuildHistogram() \
    .Name(name) \
    .Help(help) \
    .Register(*registry_) \
    .LabelNames(label, __VA_ARGS__)),
        _make_histogram_family(_init_histogram_familys)
        dummy_() {
    for (
        auto pair : rocksdb::TickersNameMap
            ) {
        std::for_each(pair.second.begin(), pair.second.end(),
                      [](std::string::value_type &c) {
                          c = c == '.' ? c : '-';
                      });

        tickers_names_.insert(pair);
    }
    for (
        auto pair : rocksdb::HistogramsNameMap
            ) {
        std::for_each(pair.second.begin(), pair.second.end(),
                      [](std::string::value_type &c) {
                          c = c == '.' ? c : '-';
                      });
        histograms_names_.insert(pair);
    }
}


void RocksdbStatistics::FlushMetrics(rocksdb::DB &db, const std::string &name,
                                     const std::vector<rocksdb::ColumnFamilyHandle *> &db_cfs) {
    auto statistics = db.GetDBOptions().statistics;
    for (auto &pair: this->tickers_names_) {
        auto v = statistics->getAndResetTickerCount(pair.first);
        FlushEngineTickerMetrics(pair.first, v, name);
    }

    for (auto &pair: this->histograms_names_) {
        rocksdb::HistogramData hisdata;
        statistics->histogramData(pair.first, &hisdata);
        FlushEngineHistogramMetrics(pair.first, hisdata, name);
    }

    FlushEngineProperties(db, name, db_cfs);
}


void RocksdbStatistics::FlushEngineTickerMetrics(rocksdb::Tickers t, const uint64_t value, const std::string &name) {
    int64_t v = value;
    if (v < 0) {
        std::cout << "ticker is overflow, ticker: " << tickers_names_[t] << ";value" << value << std::endl;
    }

    switch (t) {
        case rocksdb::Tickers::BLOCK_CACHE_MISS :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_miss"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_HIT :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_hit"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_ADD :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_add"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_ADD_FAILURES :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_add_failures"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_INDEX_MISS :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_index_miss"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_INDEX_HIT :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_index_hit"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_INDEX_ADD :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_index_add"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_INDEX_BYTES_INSERT :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_index_bytes_insert"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_INDEX_BYTES_EVICT :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_index_bytes_evict"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_FILTER_MISS :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_filter_miss"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_FILTER_HIT :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_filter_hit"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_FILTER_ADD :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_filter_add"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_FILTER_BYTES_INSERT :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_filter_bytes_insert"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_FILTER_BYTES_EVICT :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_filter_bytes_evict"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_DATA_MISS :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_data_miss"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_DATA_HIT :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_data_hit"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_DATA_ADD :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_data_add"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_DATA_BYTES_INSERT :
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                    .WithLabelValues({name, "block_cache_data_bytes_insert"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_BYTES_READ :
            STORE_ENGINE_FLOW_VEC
                    .WithLabelValues({name, "block_cache_bytes_read"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOCK_CACHE_BYTES_WRITE :
            STORE_ENGINE_FLOW_VEC
                    .WithLabelValues({name, "block_cache_bytes_write"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOOM_FILTER_USEFUL :
            STORE_ENGINE_BLOOM_EFFICIENCY_VEC
                    .WithLabelValues({name, "bloom_filter_useful"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::MEMTABLE_HIT :
            STORE_ENGINE_MEMTABLE_EFFICIENCY_VEC
                    .WithLabelValues({name, "memtable_hit"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::MEMTABLE_MISS :
            STORE_ENGINE_MEMTABLE_EFFICIENCY_VEC
                    .WithLabelValues({name, "memtable_miss"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::GET_HIT_L0 :
            STORE_ENGINE_GET_SERVED_VEC
                    .WithLabelValues({name, "get_hit_l0"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::GET_HIT_L1 :
            STORE_ENGINE_GET_SERVED_VEC
                    .WithLabelValues({name, "get_hit_l1"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::GET_HIT_L2_AND_UP :
            STORE_ENGINE_GET_SERVED_VEC
                    .WithLabelValues({name, "get_hit_l2_and_up"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::COMPACTION_KEY_DROP_NEWER_ENTRY :
            STORE_ENGINE_COMPACTION_DROP_VEC
                    .WithLabelValues({name, "compaction_key_drop_newer_entry"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::COMPACTION_KEY_DROP_OBSOLETE :
            STORE_ENGINE_COMPACTION_DROP_VEC
                    .WithLabelValues({name, "compaction_key_drop_obsolete"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::COMPACTION_KEY_DROP_RANGE_DEL :
            STORE_ENGINE_COMPACTION_DROP_VEC
                    .WithLabelValues({name, "compaction_key_drop_range_del"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::COMPACTION_RANGE_DEL_DROP_OBSOLETE :
            STORE_ENGINE_COMPACTION_DROP_VEC
                    .WithLabelValues({name, "compaction_range_del_drop_obsolete"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE :
            STORE_ENGINE_COMPACTION_DROP_VEC
                    .WithLabelValues({name, "compaction_optimized_del_drop_obsolete"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::NUMBER_KEYS_WRITTEN :
            STORE_ENGINE_FLOW_VEC
                    .WithLabelValues({name, "number_keys_written"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::NUMBER_KEYS_READ :
            STORE_ENGINE_FLOW_VEC
                    .WithLabelValues({name, "number_keys_read"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::NUMBER_KEYS_UPDATED :
            STORE_ENGINE_FLOW_VEC
                    .WithLabelValues({name, "number_keys_updated"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BYTES_WRITTEN :
            STORE_ENGINE_FLOW_VEC
                    .WithLabelValues({name, "bytes_written"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BYTES_READ :
            STORE_ENGINE_FLOW_VEC
                    .WithLabelValues({name, "bytes_read"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::NUMBER_DB_SEEK :
            STORE_ENGINE_LOCATE_VEC
                    .WithLabelValues({name, "number_db_seek"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::NUMBER_DB_NEXT :
            STORE_ENGINE_LOCATE_VEC
                    .WithLabelValues({name, "number_db_next"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::NUMBER_DB_PREV :
            STORE_ENGINE_LOCATE_VEC
                    .WithLabelValues({name, "number_db_prev"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::NUMBER_DB_SEEK_FOUND :
            STORE_ENGINE_LOCATE_VEC
                    .WithLabelValues({name, "number_db_seek_found"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::NUMBER_DB_NEXT_FOUND :
            STORE_ENGINE_LOCATE_VEC
                    .WithLabelValues({name, "number_db_next_found"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::NUMBER_DB_PREV_FOUND :
            STORE_ENGINE_LOCATE_VEC
                    .WithLabelValues({name, "number_db_prev_found"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::ITER_BYTES_READ :
            STORE_ENGINE_FLOW_VEC
                    .WithLabelValues({name, "iter_bytes_read"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::NO_FILE_CLOSES :
            STORE_ENGINE_FILE_STATUS_VEC
                    .WithLabelValues({name, "no_file_closes"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::NO_FILE_OPENS :
            STORE_ENGINE_FILE_STATUS_VEC
                    .WithLabelValues({name, "no_file_opens"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::NO_FILE_ERRORS :
            STORE_ENGINE_FILE_STATUS_VEC
                    .WithLabelValues({name, "no_file_errors"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::STALL_MICROS :
            STORE_ENGINE_STALL_MICROS
                    .WithLabelValues({name})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOOM_FILTER_PREFIX_CHECKED :
            STORE_ENGINE_BLOOM_EFFICIENCY_VEC
                    .WithLabelValues({name, "bloom_filter_prefix_checked"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL :
            STORE_ENGINE_BLOOM_EFFICIENCY_VEC
                    .WithLabelValues({name, "bloom_filter_prefix_useful"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::WAL_FILE_SYNCED :
            STORE_ENGINE_WAL_FILE_SYNCED
                    .WithLabelValues({name})
                    .Increment(v);
            break;
        case rocksdb::Tickers::WAL_FILE_BYTES :
            STORE_ENGINE_FLOW_VEC
                    .WithLabelValues({name, "wal_file_bytes"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::WRITE_DONE_BY_SELF :
            STORE_ENGINE_WRITE_SERVED_VEC
                    .WithLabelValues({name, "write_done_by_self"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::WRITE_DONE_BY_OTHER :
            STORE_ENGINE_WRITE_SERVED_VEC
                    .WithLabelValues({name, "write_done_by_other"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::WRITE_TIMEDOUT :
            STORE_ENGINE_WRITE_SERVED_VEC
                    .WithLabelValues({name, "write_timeout"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::WRITE_WITH_WAL :
            STORE_ENGINE_WRITE_SERVED_VEC
                    .WithLabelValues({name, "write_with_wal"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::COMPACT_READ_BYTES :
            STORE_ENGINE_COMPACTION_FLOW_VEC
                    .WithLabelValues({name, "compact_bytes_read"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::COMPACT_WRITE_BYTES :
            STORE_ENGINE_COMPACTION_FLOW_VEC
                    .WithLabelValues({name, "compact_bytes_written"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::FLUSH_WRITE_BYTES :
            STORE_ENGINE_FLOW_VEC
                    .WithLabelValues({name, "flush_write_bytes"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::MERGE_OPERATION_TOTAL_TIME :
            STORE_ENGINE_MERGE_TOTAL_TIME
                    .WithLabelValues({name, "merge_operation_total_time"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::READ_AMP_ESTIMATE_USEFUL_BYTES :
            STORE_ENGINE_READ_AMP_FLOW_VEC
                    .WithLabelValues({name, "read_amp_estimate_useful_bytes"})
                    .Increment(v);
            break;
        case rocksdb::Tickers::READ_AMP_TOTAL_READ_BYTES :
            STORE_ENGINE_READ_AMP_FLOW_VEC
                    .WithLabelValues({name, "read_amp_total_read_bytes"})
                    .Increment(v);
            break;
        default:
            break;
    }
}

void RocksdbStatistics::FlushEngineHistogramMetrics(rocksdb::Histograms t, const rocksdb::HistogramData &value,
                                                    const std::string &name) {
    switch (t) {
        case rocksdb::Histograms::DB_GET :
            STORE_ENGINE_GET_MICROS_VEC
                    .WithLabelValues({name, "get_median"})
                    .Set(value.median);
            STORE_ENGINE_GET_MICROS_VEC
                    .WithLabelValues({name, "get_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_GET_MICROS_VEC
                    .WithLabelValues({name, "get_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_GET_MICROS_VEC
                    .WithLabelValues({name, "get_average"})
                    .Set(value.average);
            STORE_ENGINE_GET_MICROS_VEC
                    .WithLabelValues({name, "get_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_GET_MICROS_VEC
                    .WithLabelValues({name, "get_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::DB_WRITE :
            STORE_ENGINE_WRITE_MICROS_VEC
                    .WithLabelValues({name, "write_median"})
                    .Set(value.median);
            STORE_ENGINE_WRITE_MICROS_VEC
                    .WithLabelValues({name, "write_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_WRITE_MICROS_VEC
                    .WithLabelValues({name, "write_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_WRITE_MICROS_VEC
                    .WithLabelValues({name, "write_average"})
                    .Set(value.average);
            STORE_ENGINE_WRITE_MICROS_VEC
                    .WithLabelValues({name, "write_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_WRITE_MICROS_VEC
                    .WithLabelValues({name, "write_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::COMPACTION_TIME :
            STORE_ENGINE_COMPACTION_TIME_VEC
                    .WithLabelValues({name, "compaction_time_median"})
                    .Set(value.median);
            STORE_ENGINE_COMPACTION_TIME_VEC
                    .WithLabelValues({name, "compaction_time_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_COMPACTION_TIME_VEC
                    .WithLabelValues({name, "compaction_time_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_COMPACTION_TIME_VEC
                    .WithLabelValues({name, "compaction_time_average"})
                    .Set(value.average);
            STORE_ENGINE_COMPACTION_TIME_VEC
                    .WithLabelValues({name, "compaction_time_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_COMPACTION_TIME_VEC
                    .WithLabelValues({name, "compaction_time_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::TABLE_SYNC_MICROS :
            STORE_ENGINE_TABLE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "table_sync_median"})
                    .Set(value.median);
            STORE_ENGINE_TABLE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "table_sync_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_TABLE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "table_sync_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_TABLE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "table_sync_average"})
                    .Set(value.average);
            STORE_ENGINE_TABLE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "table_sync_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_TABLE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "table_sync_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::COMPACTION_OUTFILE_SYNC_MICROS :
            STORE_ENGINE_COMPACTION_OUTFILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "compaction_outfile_sync_median"})
                    .Set(value.median);
            STORE_ENGINE_COMPACTION_OUTFILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "compaction_outfile_sync_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_COMPACTION_OUTFILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "compaction_outfile_sync_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_COMPACTION_OUTFILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "compaction_outfile_sync_average"})
                    .Set(value.average);
            STORE_ENGINE_COMPACTION_OUTFILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "compaction_outfile_sync_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_COMPACTION_OUTFILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "compaction_outfile_sync_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::WAL_FILE_SYNC_MICROS :
            STORE_ENGINE_WAL_FILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "wal_file_sync_median"})
                    .Set(value.median);
            STORE_ENGINE_WAL_FILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "wal_file_sync_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_WAL_FILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "wal_file_sync_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_WAL_FILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "wal_file_sync_average"})
                    .Set(value.average);
            STORE_ENGINE_WAL_FILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "wal_file_sync_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_WAL_FILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "wal_file_sync_max"})
                    .Set(value.standard_deviation);
            break;
        case rocksdb::Histograms::MANIFEST_FILE_SYNC_MICROS :
            STORE_ENGINE_MANIFEST_FILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "manifest_file_sync_median"})
                    .Set(value.median);
            STORE_ENGINE_MANIFEST_FILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "manifest_file_sync_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_MANIFEST_FILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "manifest_file_sync_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_MANIFEST_FILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "manifest_file_sync_average"})
                    .Set(value.average);
            STORE_ENGINE_MANIFEST_FILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "manifest_file_sync_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_MANIFEST_FILE_SYNC_MICROS_VEC
                    .WithLabelValues({name, "manifest_file_sync_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::STALL_L0_SLOWDOWN_COUNT :
            STORE_ENGINE_STALL_L0_SLOWDOWN_COUNT_VEC
                    .WithLabelValues({name, "stall_l0_slowdown_count_median"})
                    .Set(value.median);
            STORE_ENGINE_STALL_L0_SLOWDOWN_COUNT_VEC
                    .WithLabelValues({name, "stall_l0_slowdown_count_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_STALL_L0_SLOWDOWN_COUNT_VEC
                    .WithLabelValues({name, "stall_l0_slowdown_count_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_STALL_L0_SLOWDOWN_COUNT_VEC
                    .WithLabelValues({name, "stall_l0_slowdown_count_average"})
                    .Set(value.average);
            STORE_ENGINE_STALL_L0_SLOWDOWN_COUNT_VEC
                    .WithLabelValues({name, "stall_l0_slowdown_count_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_STALL_L0_SLOWDOWN_COUNT_VEC
                    .WithLabelValues({name, "stall_l0_slowdown_count_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::STALL_MEMTABLE_COMPACTION_COUNT :
            STORE_ENGINE_STALL_MEMTABLE_COMPACTION_COUNT_VEC
                    .WithLabelValues({name, "stall_memtable_compaction_count_median"})
                    .Set(value.median);
            STORE_ENGINE_STALL_MEMTABLE_COMPACTION_COUNT_VEC
                    .WithLabelValues({name, "stall_memtable_compaction_count_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_STALL_MEMTABLE_COMPACTION_COUNT_VEC
                    .WithLabelValues({name, "stall_memtable_compaction_count_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_STALL_MEMTABLE_COMPACTION_COUNT_VEC
                    .WithLabelValues({name, "stall_memtable_compaction_count_average"})
                    .Set(value.average);
            STORE_ENGINE_STALL_MEMTABLE_COMPACTION_COUNT_VEC
                    .WithLabelValues({name, "stall_memtable_compaction_count_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_STALL_MEMTABLE_COMPACTION_COUNT_VEC
                    .WithLabelValues({name, "stall_memtable_compaction_count_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::STALL_L0_NUM_FILES_COUNT :
            STORE_ENGINE_STALL_LO_NUM_FILES_COUNT_VEC
                    .WithLabelValues({name, "stall_l0_num_files_count_median"})
                    .Set(value.median);
            STORE_ENGINE_STALL_LO_NUM_FILES_COUNT_VEC
                    .WithLabelValues({name, "stall_l0_num_files_count_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_STALL_LO_NUM_FILES_COUNT_VEC
                    .WithLabelValues({name, "stall_l0_num_files_count_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_STALL_LO_NUM_FILES_COUNT_VEC
                    .WithLabelValues({name, "stall_l0_num_files_count_average"})
                    .Set(value.average);
            STORE_ENGINE_STALL_LO_NUM_FILES_COUNT_VEC
                    .WithLabelValues({name, "stall_l0_num_files_count_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_STALL_LO_NUM_FILES_COUNT_VEC
                    .WithLabelValues({name, "stall_l0_num_files_count_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::HARD_RATE_LIMIT_DELAY_COUNT :
            STORE_ENGINE_HARD_RATE_LIMIT_DELAY_COUNT_VEC
                    .WithLabelValues({name, "hard_rate_limit_delay_median"})
                    .Set(value.median);
            STORE_ENGINE_HARD_RATE_LIMIT_DELAY_COUNT_VEC
                    .WithLabelValues({name, "hard_rate_limit_delay_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_HARD_RATE_LIMIT_DELAY_COUNT_VEC
                    .WithLabelValues({name, "hard_rate_limit_delay_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_HARD_RATE_LIMIT_DELAY_COUNT_VEC
                    .WithLabelValues({name, "hard_rate_limit_delay_average"})
                    .Set(value.average);
            STORE_ENGINE_HARD_RATE_LIMIT_DELAY_COUNT_VEC
                    .WithLabelValues({name, "hard_rate_limit_delay_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_HARD_RATE_LIMIT_DELAY_COUNT_VEC
                    .WithLabelValues({name, "hard_rate_limit_delay_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::SOFT_RATE_LIMIT_DELAY_COUNT :
            STORE_ENGINE_SOFT_RATE_LIMIT_DELAY_COUNT_VEC
                    .WithLabelValues({name, "soft_rate_limit_delay_median"})
                    .Set(value.median);
            STORE_ENGINE_SOFT_RATE_LIMIT_DELAY_COUNT_VEC
                    .WithLabelValues({name, "soft_rate_limit_delay_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_SOFT_RATE_LIMIT_DELAY_COUNT_VEC
                    .WithLabelValues({name, "soft_rate_limit_delay_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_SOFT_RATE_LIMIT_DELAY_COUNT_VEC
                    .WithLabelValues({name, "soft_rate_limit_delay_average"})
                    .Set(value.average);
            STORE_ENGINE_SOFT_RATE_LIMIT_DELAY_COUNT_VEC
                    .WithLabelValues({name, "soft_rate_limit_delay_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_SOFT_RATE_LIMIT_DELAY_COUNT_VEC
                    .WithLabelValues({name, "soft_rate_limit_delay_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::NUM_FILES_IN_SINGLE_COMPACTION :
            STORE_ENGINE_NUM_FILES_IN_SINGLE_COMPACTION_VEC
                    .WithLabelValues({name, "num_files_in_single_compaction_median"})
                    .Set(value.median);
            STORE_ENGINE_NUM_FILES_IN_SINGLE_COMPACTION_VEC
                    .WithLabelValues({name, "num_files_in_single_compaction_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_NUM_FILES_IN_SINGLE_COMPACTION_VEC
                    .WithLabelValues({name, "num_files_in_single_compaction_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_NUM_FILES_IN_SINGLE_COMPACTION_VEC
                    .WithLabelValues({name, "num_files_in_single_compaction_average"})
                    .Set(value.average);
            STORE_ENGINE_NUM_FILES_IN_SINGLE_COMPACTION_VEC
                    .WithLabelValues({name, "num_files_in_single_compaction_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_NUM_FILES_IN_SINGLE_COMPACTION_VEC
                    .WithLabelValues({name, "num_files_in_single_compaction_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::DB_SEEK :
            STORE_ENGINE_SEEK_MICROS_VEC
                    .WithLabelValues({name, "seek_median"})
                    .Set(value.median);
            STORE_ENGINE_SEEK_MICROS_VEC
                    .WithLabelValues({name, "seek_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_SEEK_MICROS_VEC
                    .WithLabelValues({name, "seek_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_SEEK_MICROS_VEC
                    .WithLabelValues({name, "seek_average"})
                    .Set(value.average);
            STORE_ENGINE_SEEK_MICROS_VEC
                    .WithLabelValues({name, "seek_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_SEEK_MICROS_VEC
                    .WithLabelValues({name, "seek_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::WRITE_STALL :
            STORE_ENGINE_WRITE_STALL_VEC
                    .WithLabelValues({name, "write_stall_median"})
                    .Set(value.median);
            STORE_ENGINE_WRITE_STALL_VEC
                    .WithLabelValues({name, "write_stall_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_WRITE_STALL_VEC
                    .WithLabelValues({name, "write_stall_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_WRITE_STALL_VEC
                    .WithLabelValues({name, "write_stall_average"})
                    .Set(value.average);
            STORE_ENGINE_WRITE_STALL_VEC
                    .WithLabelValues({name, "write_stall_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_WRITE_STALL_VEC
                    .WithLabelValues({name, "write_stall_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::SST_READ_MICROS :
            STORE_ENGINE_SST_READ_MICROS_VEC
                    .WithLabelValues({name, "sst_read_micros_median"})
                    .Set(value.median);
            STORE_ENGINE_SST_READ_MICROS_VEC
                    .WithLabelValues({name, "sst_read_micros_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_SST_READ_MICROS_VEC
                    .WithLabelValues({name, "sst_read_micros_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_SST_READ_MICROS_VEC
                    .WithLabelValues({name, "sst_read_micros_average"})
                    .Set(value.average);
            STORE_ENGINE_SST_READ_MICROS_VEC
                    .WithLabelValues({name, "sst_read_micros_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_SST_READ_MICROS_VEC
                    .WithLabelValues({name, "sst_read_micros_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::NUM_SUBCOMPACTIONS_SCHEDULED :
            STORE_ENGINE_NUM_SUBCOMPACTION_SCHEDULED_VEC
                    .WithLabelValues({name, "num_subcompaction_scheduled_median"})
                    .Set(value.median);
            STORE_ENGINE_NUM_SUBCOMPACTION_SCHEDULED_VEC
                    .WithLabelValues({name, "num_subcompaction_scheduled_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_NUM_SUBCOMPACTION_SCHEDULED_VEC
                    .WithLabelValues({name, "num_subcompaction_scheduled_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_NUM_SUBCOMPACTION_SCHEDULED_VEC
                    .WithLabelValues({name, "num_subcompaction_scheduled_average"})
                    .Set(value.average);
            STORE_ENGINE_NUM_SUBCOMPACTION_SCHEDULED_VEC
                    .WithLabelValues({name, "num_subcompaction_scheduled_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_NUM_SUBCOMPACTION_SCHEDULED_VEC
                    .WithLabelValues({name, "num_subcompaction_scheduled_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::BYTES_PER_READ :
            STORE_ENGINE_BYTES_PER_READ_VEC
                    .WithLabelValues({name, "bytes_per_read_median"})
                    .Set(value.median);
            STORE_ENGINE_BYTES_PER_READ_VEC
                    .WithLabelValues({name, "bytes_per_read_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_BYTES_PER_READ_VEC
                    .WithLabelValues({name, "bytes_per_read_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_BYTES_PER_READ_VEC
                    .WithLabelValues({name, "bytes_per_read_average"})
                    .Set(value.average);
            STORE_ENGINE_BYTES_PER_READ_VEC
                    .WithLabelValues({name, "bytes_per_read_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_BYTES_PER_READ_VEC
                    .WithLabelValues({name, "bytes_per_read_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::BYTES_PER_WRITE :
            STORE_ENGINE_BYTES_PER_WRITE_VEC
                    .WithLabelValues({name, "bytes_per_write_median"})
                    .Set(value.median);
            STORE_ENGINE_BYTES_PER_WRITE_VEC
                    .WithLabelValues({name, "bytes_per_write_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_BYTES_PER_WRITE_VEC
                    .WithLabelValues({name, "bytes_per_write_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_BYTES_PER_WRITE_VEC
                    .WithLabelValues({name, "bytes_per_write_average"})
                    .Set(value.average);
            STORE_ENGINE_BYTES_PER_WRITE_VEC
                    .WithLabelValues({name, "bytes_per_write_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_BYTES_PER_WRITE_VEC
                    .WithLabelValues({name, "bytes_per_write_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::BYTES_COMPRESSED :
            STORE_ENGINE_BYTES_COMPRESSED_VEC
                    .WithLabelValues({name, "bytes_compressed_median"})
                    .Set(value.median);
            STORE_ENGINE_BYTES_COMPRESSED_VEC
                    .WithLabelValues({name, "bytes_compressed_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_BYTES_COMPRESSED_VEC
                    .WithLabelValues({name, "bytes_compressed_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_BYTES_COMPRESSED_VEC
                    .WithLabelValues({name, "bytes_compressed_average"})
                    .Set(value.average);
            STORE_ENGINE_BYTES_COMPRESSED_VEC
                    .WithLabelValues({name, "bytes_compressed_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_BYTES_COMPRESSED_VEC
                    .WithLabelValues({name, "bytes_compressed_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::BYTES_DECOMPRESSED :
            STORE_ENGINE_BYTES_DECOMPRESSED_VEC
                    .WithLabelValues({name, "bytes_decompressed_median"})
                    .Set(value.median);
            STORE_ENGINE_BYTES_DECOMPRESSED_VEC
                    .WithLabelValues({name, "bytes_decompressed_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_BYTES_DECOMPRESSED_VEC
                    .WithLabelValues({name, "bytes_decompressed_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_BYTES_DECOMPRESSED_VEC
                    .WithLabelValues({name, "bytes_decompressed_average"})
                    .Set(value.average);
            STORE_ENGINE_BYTES_DECOMPRESSED_VEC
                    .WithLabelValues({name, "bytes_decompressed_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_BYTES_DECOMPRESSED_VEC
                    .WithLabelValues({name, "bytes_decompressed_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::COMPRESSION_TIMES_NANOS :
            STORE_ENGINE_COMPRESSION_TIMES_NANOS_VEC
                    .WithLabelValues({name, "compression_time_nanos_median"})
                    .Set(value.median);
            STORE_ENGINE_COMPRESSION_TIMES_NANOS_VEC
                    .WithLabelValues({name, "compression_time_nanos_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_COMPRESSION_TIMES_NANOS_VEC
                    .WithLabelValues({name, "compression_time_nanos_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_COMPRESSION_TIMES_NANOS_VEC
                    .WithLabelValues({name, "compression_time_nanos_average"})
                    .Set(value.average);
            STORE_ENGINE_COMPRESSION_TIMES_NANOS_VEC
                    .WithLabelValues({name, "compression_time_nanos_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_COMPRESSION_TIMES_NANOS_VEC
                    .WithLabelValues({name, "compression_time_nanos_max"})
                    .Set(value.max);
            break;
        case rocksdb::Histograms::DECOMPRESSION_TIMES_NANOS :
            STORE_ENGINE_DECOMPRESSION_TIMES_NANOS_VEC
                    .WithLabelValues({name, "decompression_time_nanos_median"})
                    .Set(value.median);
            STORE_ENGINE_DECOMPRESSION_TIMES_NANOS_VEC
                    .WithLabelValues({name, "decompression_time_nanos_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_DECOMPRESSION_TIMES_NANOS_VEC
                    .WithLabelValues({name, "decompression_time_nanos_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_DECOMPRESSION_TIMES_NANOS_VEC
                    .WithLabelValues({name, "decompression_time_nanos_average"})
                    .Set(value.average);
            STORE_ENGINE_DECOMPRESSION_TIMES_NANOS_VEC
                    .WithLabelValues({name, "decompression_time_nanos_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_DECOMPRESSION_TIMES_NANOS_VEC
                    .WithLabelValues({name, "decompression_time_nanos_max"})
                    .Set(value.max);
            break;

        case rocksdb::Histograms::READ_NUM_MERGE_OPERANDS :
            STORE_ENGINE_READ_MERGE_OPERANDS
                    .WithLabelValues({name, "read_num_merge_operands_median"})
                    .Set(value.median);
            STORE_ENGINE_READ_MERGE_OPERANDS
                    .WithLabelValues({name, "read_num_merge_operands_percentile95"})
                    .Set(value.percentile95);
            STORE_ENGINE_READ_MERGE_OPERANDS
                    .WithLabelValues({name, "read_num_merge_operands_percentile99"})
                    .Set(value.percentile99);
            STORE_ENGINE_READ_MERGE_OPERANDS
                    .WithLabelValues({name, "read_num_merge_operands_average"})
                    .Set(value.average);
            STORE_ENGINE_READ_MERGE_OPERANDS
                    .WithLabelValues({name, "read_num_merge_operands_standard_deviation"})
                    .Set(value.standard_deviation);
            STORE_ENGINE_READ_MERGE_OPERANDS
                    .WithLabelValues({name, "read_num_merge_operands_max"})
                    .Set(value.max);
            break;
        default:
            break;
    }
}

void RocksdbStatistics::FlushEngineProperties(rocksdb::DB &db, const std::string &name,
                                              const std::vector<rocksdb::ColumnFamilyHandle *> &db_cfs) {
    class RocksdbPropertiesAppend {
    public:
        std::string operator()(const std::string &prefix, const std::string &append) {
            return std::move(prefix + append);
        }
    };

    static const std::string ROCKSDB_TOTAL_SST_FILES_SIZE = rocksdb::DB::Properties::kTotalSstFilesSize;
    static const std::string ROCKSDB_TABLE_READERS_MEM = rocksdb::DB::Properties::kEstimateTableReadersMem;
    static const std::string ROCKSDB_CUR_SIZE_ALL_MEM_TABLES = rocksdb::DB::Properties::kCurSizeAllMemTables;
    static const std::string ROCKSDB_ESTIMATE_NUM_KEYS = rocksdb::DB::Properties::kEstimateNumKeys;
    static const std::string ROCKSDB_PENDING_COMPACTION_BYTES = rocksdb::DB::Properties::kEstimatePendingCompactionBytes;
    static const std::string ROCKSDB_NUM_SNAPSHOTS = rocksdb::DB::Properties::kNumSnapshots;
    static const std::string ROCKSDB_OLDEST_SNAPSHOT_TIME = rocksdb::DB::Properties::kOldestSnapshotTime;
    static const std::string ROCKSDB_NUM_IMMUTABLE_MEM_TABLE = rocksdb::DB::Properties::kNumImmutableMemTable;
    static const std::string ROCKSDB_BLOCK_CACHE_USAGE = rocksdb::DB::Properties::kBlockCacheUsage;
    static const std::string ROCKSDB_COMPRESSION_RATIO_AT_LEVEL = rocksdb::DB::Properties::kCompressionRatioAtLevelPrefix;
    static const std::string ROCKSDB_NUM_FILES_AT_LEVEL = rocksdb::DB::Properties::kNumFilesAtLevelPrefix;


    uint64_t value;
    for (const auto &handle : db_cfs) {
        auto cf = handle->GetName();
        // It is important to monitor each cf's size, especially the "raft" and "lock" column
        // families.
        if (!db.GetIntProperty(handle, ROCKSDB_TOTAL_SST_FILES_SIZE, &value)) {
            std::cout << "rocksdb is too old, missing total-sst-files-size property" << std::endl;
        } else {
            STORE_ENGINE_SIZE_GAUGE_VEC
                    .WithLabelValues({name, cf})
                    .Set(value);
        }

        // For block cache usage
        if (db.GetIntProperty(handle, ROCKSDB_BLOCK_CACHE_USAGE, &value)) {
            STORE_ENGINE_BLOCK_CACHE_USAGE_GAUGE_VEC
                    .WithLabelValues({name, cf})
                    .Set(value);
        }
        // For block cache usage
        if (db.GetIntProperty(handle, ROCKSDB_BLOCK_CACHE_USAGE, &value)) {
            STORE_ENGINE_BLOCK_CACHE_USAGE_GAUGE_VEC
                    .WithLabelValues({name, cf})
                    .Set(value);
        }

        // TODO: find a better place to record these metrics.
        // Refer: https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB
        // For index and filter blocks memory
        if (db.GetIntProperty(handle, ROCKSDB_TABLE_READERS_MEM, &value)) {
            STORE_ENGINE_MEMORY_GAUGE_VEC
                    .WithLabelValues({name, cf, "readers-mem"})
                    .Set(value);
        }

        // For memtable
        if (db.GetIntProperty(handle, ROCKSDB_CUR_SIZE_ALL_MEM_TABLES, &value)) {
            STORE_ENGINE_MEMORY_GAUGE_VEC
                    .WithLabelValues({name, cf, "mem-tables"})
                    .Set(value);
        }

        // TODO: WithLabelValues cache usage and pinned usage.

        if (db.GetIntProperty(handle, ROCKSDB_ESTIMATE_NUM_KEYS, &value)) {
            STORE_ENGINE_ESTIMATE_NUM_KEYS_VEC
                    .WithLabelValues({name, cf})
                    .Set(value);
        }

        // Pending compaction bytes
        if (db.GetIntProperty(handle, ROCKSDB_PENDING_COMPACTION_BYTES, &value)) {
            STORE_ENGINE_PENDING_COMACTION_BYTES_VEC
                    .WithLabelValues({name, cf})
                    .Set(value);
        }

        auto levels = db.GetOptions(handle).num_levels;
        std::string str_value;
        for (int level = 0; level < levels; level++) {
            // Compression ratio at levels
            auto str_level = std::to_string(level);
            if (db.GetProperty(handle, RocksdbPropertiesAppend()(ROCKSDB_COMPRESSION_RATIO_AT_LEVEL, str_level),
                               &str_value)) {
                auto v = std::atof(str_value.c_str());
                if (v >= 0.0) {
                    STORE_ENGINE_COMPRESSION_RATIO_VEC
                            .WithLabelValues({name, cf, str_level})
                            .Set(v);
                }
            }

            if (db.GetIntProperty(handle, RocksdbPropertiesAppend()(ROCKSDB_NUM_FILES_AT_LEVEL, str_level),
                                  &value)) {
                STORE_ENGINE_NUM_FILES_AT_LEVEL_VEC
                        .WithLabelValues({name, cf, str_level})
                        .Set(value);
            }
        }

        // Num immutable mem-table
        if (db.GetIntProperty(handle, ROCKSDB_NUM_IMMUTABLE_MEM_TABLE, &value)) {
            STORE_ENGINE_NUM_IMMUTABLE_MEM_TABLE_VEC
                    .WithLabelValues({name, cf})
                    .Set(value);
        }
    }

// For snapshot
    if (db.GetIntProperty(ROCKSDB_NUM_SNAPSHOTS, &value)) {
        STORE_ENGINE_NUM_SNAPSHOTS_GAUGE_VEC
                .WithLabelValues({name})
                .Set(value);
    }

    if (db.GetIntProperty(ROCKSDB_OLDEST_SNAPSHOT_TIME, &value)) {
        // RocksDB returns 0 if no snapshots.
        using namespace std::chrono;
        uint64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
        uint64_t v = value > 0 && now > value ? now - value : 0;

        STORE_ENGINE_OLDEST_SNAPSHOT_DURATION_GAUGE_VEC
                .WithLabelValues({name})
                .Set(v);
    }
}
