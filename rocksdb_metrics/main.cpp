#include <iostream>
#include <random>
#include <chrono>
#include <thread>

#include <sys/stat.h>
#include <sys/types.h>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>

#include "metrics.hh"
#include "rocksdb_metrics.hh"
#include "system_metrics.hh"
#include "benchmark.hh"


#include <gflags/gflags.h>

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

DEFINE_string(
        benchmarks,
        "put,"
        "batch,",
        "\tput    -- use db.put to test\n"
        "\tbatch  -- use writebatch to test\n");
DEFINE_int32(threads, 1, "Number of threads");
DEFINE_int64(nums, 10000, "Number of key nums to write");
DEFINE_int32(value_size, 100, "the value size");
DEFINE_int32(prometheus_port, 8080, "prometheus port");
DEFINE_int32(rocksdb_num, 1, "rocksdb's nums for test");
DEFINE_int32(rocksdb_columns, 1, "every rocksdb's column familys");
DEFINE_int32(batch_num, 1, "if use batch to test, it's batch nums");
DEFINE_bool(sync, true, "rockdb sync or not");
DEFINE_bool(disable_wal, false, "disable rockdb wal or not");


DEFINE_int32(max_subcompactions, 10, "options max subcompactions");
DEFINE_int32(max_background_compactions, 10, "options max background compactions");
DEFINE_int32(write_buffer_size, 128, "options write buffer size (MB)");
DEFINE_int32(max_bytes_for_level_base, 1280, "options max bytes for level base (MB)");
DEFINE_int32(level0_slowdown_writes_trigger, 10, "options level0 slowdown writes trigger");

class RocksdbWarpper {
    static const size_t KB = 1024;
    static const size_t MB = 1024 * 1024;
    static const size_t GB = 1024 * 1024 * 1024;
public:

    RocksdbWarpper(int column_family_num, const std::string &dbpath,
                   std::shared_ptr<StatisticsEventListener> listener)
            : column_family_num_(column_family_num),
              dbpath_(dbpath), statistics_event_listener_(listener) {
        Open();
    }

    ~RocksdbWarpper() {
        for (auto &cf : db_cfs_) {
            delete cf;
        }
        delete db_;
    }

    rocksdb::DB *GetDB() {
        return db_;
    }

    std::vector<rocksdb::ColumnFamilyHandle *> GetColumnFamilyHandle() {
        return db_cfs_;
    }

private:

    void Open() {
        auto options = DefaultOptions();
        options.listeners.push_back(statistics_event_listener_);
        rocksdb::Status s = rocksdb::DB::Open(options, dbpath_, DefaultColumnFamilies(column_family_num_), &db_cfs_,
                                              &db_);
        assert(s.ok());
    }

    static rocksdb::Options DefaultOptions() {
        //db options
        rocksdb::Options options;
        options.create_if_missing = true;
        options.create_missing_column_families = true;
        options.write_buffer_size = FLAGS_write_buffer_size * MB;

        /*
         * level0 --> write_buffer_size
         *            * min_write_buffer_number_to_merge
         *            * level0_file_num_compaction_trigger
         * level1 --> max_bytes_for_level_base
         * level2 --> level1.size * max_bytes_for_level_multiplier
         * ...
         */
        options.level0_file_num_compaction_trigger = 1;
        options.level0_slowdown_writes_trigger = FLAGS_level0_slowdown_writes_trigger;
        options.level0_stop_writes_trigger = 12;
        options.max_write_buffer_number = 4;
        options.compression_per_level = std::vector<rocksdb::CompressionType>{
                rocksdb::CompressionType::kNoCompression,
                rocksdb::CompressionType::kNoCompression,
                rocksdb::CompressionType::kNoCompression,
                rocksdb::CompressionType::kNoCompression,
                rocksdb::CompressionType::kNoCompression,
                rocksdb::CompressionType::kNoCompression,
                rocksdb::CompressionType::kNoCompression
        };

        options.max_compaction_bytes = 2 * GB; //limit for this limited will to compact
        options.min_write_buffer_number_to_merge = 1; // immutable memtable should to merge before to level0
        options.max_subcompactions = FLAGS_max_subcompactions; // level0 --> level1 compaction
        options.soft_pending_compaction_bytes_limit = 64 * GB; // slow write
        options.hard_pending_compaction_bytes_limit = 256 * GB; // stop write
        options.max_background_compactions = FLAGS_max_background_compactions;
        options.wal_bytes_per_sync = 0;
        options.WAL_ttl_seconds = 0;
        options.WAL_size_limit_MB = 0;
        options.max_open_files = -1;
        options.level_compaction_dynamic_level_bytes = true;
        options.allow_concurrent_memtable_write = true;
        options.enable_write_thread_adaptive_yield = true;

        options.statistics = rocksdb::CreateDBStatistics();
//        options.listeners.push_back(statistics_event_listener_);
        return options;
    }

    static std::vector<std::pair<std::string, size_t>> DefaultColumnFamilyNameCacheSize(int nums) {
        std::vector<std::pair<std::string, size_t>> cf_name_cache_size;
        cf_name_cache_size.push_back(std::make_pair(rocksdb::kDefaultColumnFamilyName, 1 * GB));
        for (int i = 1; i < nums; i++) {
            cf_name_cache_size.push_back(std::make_pair(std::to_string(i), 1 * GB));
        }
        return cf_name_cache_size;
    }

    static std::vector<rocksdb::ColumnFamilyDescriptor> DefaultColumnFamilies(int column_family_num) {
        std::vector<rocksdb::ColumnFamilyDescriptor> db_cf_vec;
        for (auto pair : DefaultColumnFamilyNameCacheSize(column_family_num)) {
            //table options & column options
            std::shared_ptr<rocksdb::Cache> cache = rocksdb::NewLRUCache(pair.second);
            rocksdb::BlockBasedTableOptions table_options;
            table_options.block_cache = cache;
            table_options.block_size = 16 * KB;
            table_options.cache_index_and_filter_blocks = true; // cache bloom in block cache
            table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(2, false));

            auto options = DefaultOptions();
            options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
            auto column_options = rocksdb::ColumnFamilyOptions(options);

            //column options
            column_options.max_bytes_for_level_base = FLAGS_max_bytes_for_level_base * MB;
            //column options advance
            column_options.max_compaction_bytes = 2 * GB; //limit for this limited will to compact
            column_options.min_write_buffer_number_to_merge = 1; // immutable memtable should to merge before to level0
            column_options.max_bytes_for_level_multiplier = 10;
            column_options.level_compaction_dynamic_level_bytes = false;
            column_options.target_file_size_base =
                    64 * MB; // level1 sst size; suggest is: max_bytes_for_level_base / 10
            column_options.target_file_size_multiplier = 1; // leveln sst size = level1_sst.size * n

            db_cf_vec.push_back(rocksdb::ColumnFamilyDescriptor(
                    pair.first,
                    column_options));
        }
        return db_cf_vec;
    }

private:
    int column_family_num_;
    std::string dbpath_;
    rocksdb::DB *db_;
    std::vector<rocksdb::ColumnFamilyHandle *> db_cfs_;
    std::shared_ptr<StatisticsEventListener> statistics_event_listener_;
};

class TestRocksDB {
    static const int DEFAULT_FLUSHER_RESET_INTERVAL = 60000;

public:
    TestRocksDB(const std::string &dbpath, const std::string &host)
            : metrics_service_(host), sys_statistics_(), rocksdb_statistics_(),
              statistics_stop_(false),
              statistics_event_listener_(new StatisticsEventListener("test", rocksdb_statistics_)),
              benchmark_(FLAGS_nums, FLAGS_value_size, FLAGS_sync, FLAGS_disable_wal) {
        metrics_service_.RegisterCollectableV2(sys_statistics_.GetRegistry(),
                                               rocksdb_statistics_.GetRegistry(),
                                               benchmark_.GetRegistry());
    }

    ~TestRocksDB() {
    }

    void RunTest(int rocksdb_num = 1, int column_family_nums = 1) {
        mkdir("rocksdb_data", 0755);
        for (int i = 0; i < rocksdb_num; i++) {
            auto db_ptr = std::shared_ptr<RocksdbWarpper>(new RocksdbWarpper(
                    column_family_nums,
                    std::string("rocksdb_data/rocks") + std::to_string(i),
                    statistics_event_listener_));
            rocksdbs_.push_back(db_ptr);
            for (int j = 0; j < column_family_nums; j++) {
                if (FLAGS_benchmarks == "put") {
                    benchmark_.Put(FLAGS_threads, db_ptr->GetDB(),
                                   db_ptr->GetColumnFamilyHandle()[j]);
                } else if (FLAGS_benchmarks == "batch") {
                    benchmark_.BenchPut(FLAGS_threads, FLAGS_batch_num,
                                        db_ptr->GetDB(), db_ptr->GetColumnFamilyHandle()[j]);
                } else {
                    std::cout << "Error of benchmarks params, use --benchmarks=put/batch" << std::endl;
                    exit(-1);
                }
            }
        }

        RunStatistics();
        benchmark_.Join();
    }

    void RunStatistics() {
        statistics_thread_ = std::move(std::thread(std::bind(&TestRocksDB::FlushMetrics, this)));
    }

    void FlushMetrics() {
        auto reset_time = std::chrono::system_clock::now()
                          + std::chrono::milliseconds(1 * DEFAULT_FLUSHER_RESET_INTERVAL);

        while (!statistics_stop_) {
            for (auto &db : rocksdbs_) {
                rocksdb_statistics_.FlushMetrics(*db->GetDB(), "test", db->GetColumnFamilyHandle());
                std::this_thread::sleep_for(std::chrono::seconds(2));
                sys_statistics_.FlushMetrics(".");

                auto now_time = std::chrono::system_clock::now();
                if (now_time < reset_time) {
                    db->GetDB()->GetDBOptions().statistics->Reset();
                    reset_time = now_time + std::chrono::milliseconds(1 * DEFAULT_FLUSHER_RESET_INTERVAL);
                }

            }
        }
    }

private:
    PrometheusService metrics_service_;
    SystemStatistics sys_statistics_;
    RocksdbStatistics rocksdb_statistics_;
    std::thread statistics_thread_;
    bool statistics_stop_;
    std::shared_ptr<StatisticsEventListener> statistics_event_listener_;
    Benchmark benchmark_;
    std::vector<std::shared_ptr<RocksdbWarpper>> rocksdbs_;
};


void PrintCommandLine() {
    std::cout << "prometheus port      : " << FLAGS_prometheus_port << std::endl;
    std::cout << "benchmarks type      : " << FLAGS_benchmarks << std::endl;
    std::cout << "every columns threads: " << FLAGS_threads << std::endl;
    std::cout << "value size           : " << FLAGS_value_size << std::endl;
    std::cout << "write key nums       : " << FLAGS_nums << std::endl;
    std::cout << "how many rocksdb use : " << FLAGS_rocksdb_num << std::endl;
    std::cout << "every rocksdb use columns: " << FLAGS_rocksdb_columns << std::endl;
    std::cout << "if use batch, batch num  : " << FLAGS_batch_num << std::endl;
    std::cout << "if write sync        : " << (FLAGS_sync ? "true" : "false") << std::endl;
    std::cout << "if disable wal       : " << (FLAGS_disable_wal ? "true" : "false") << std::endl;


    std::cout << "options --> max_subcompactions  : " << FLAGS_max_subcompactions << std::endl;
    std::cout << "options --> max_background_compactions : " << FLAGS_max_background_compactions << std::endl;
    std::cout << "options --> write_buffer_size, 128     : " << FLAGS_write_buffer_size << "MB" << std::endl;
    std::cout << "options --> max_bytes_for_level_base   : " << FLAGS_max_bytes_for_level_base << "MB" << std::endl;
    std::cout << "options --> level0_slowdown_writes_trigger : " << FLAGS_level0_slowdown_writes_trigger << "MB" << std::endl;

}


int main(int argc, char *argv[]) {
    ParseCommandLineFlags(&argc, &argv, true);
    PrintCommandLine();
    std::string prometheus_host = std::string("0.0.0.0:") + std::to_string(FLAGS_prometheus_port);
    TestRocksDB db("./testdb", prometheus_host);
    db.RunTest(FLAGS_rocksdb_num, FLAGS_rocksdb_columns);


    return 0;
}
