#include <iostream>
#include <random>
#include <chrono>
#include <thread>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>

#include "metrics.hh"
#include "rocksdb_metrics.hh"
#include "system_metrics.hh"


class RocksDB {
    static const size_t KB = 1024;
    static const size_t MB = 1024 * 1024;
    static const size_t GB = 1024 * 1024 * 1024;
public:
    RocksDB(const std::string &dbpath, const std::string& host)
    : dbpath_(dbpath), metrics_service_(host), sys_statistics_(), rocksdb_statistics_(), statistics_stop_(false),
      statistics_event_listener_(new StatisticsEventListener("test", rocksdb_statistics_)){

//        metrics_service_.RegisterCollectable(sys_statistics_.GetRegistry());
//        metrics_service_.RegisterCollectable(rocksdb_statistics_.GetRegistry());
        metrics_service_.RegisterCollectableV2(sys_statistics_.GetRegistry(), rocksdb_statistics_.GetRegistry());

        init_options();
        init_cf_name_cache_size();
        open();
    }

    ~RocksDB() {
        statistics_stop_ = true;
        statistics_thread_.join();
        for (auto& cf : db_cfs_) {
            delete cf;
        }
        delete db_;
    }

    void RunTest() {
        RunStatistics();
        rocksdb::ReadOptions read_options;
        rocksdb::WriteOptions write_options;
        std::default_random_engine engine;
        std::uniform_int_distribution<unsigned> u_key(0, 10000000);
        std::uniform_int_distribution<unsigned> u_sleep(100, 1000);
        std::string set_value = "value";
        std::string get_value;
        while (true) {
            auto key = std::to_string(u_key(engine));
            unsigned sleep = (u_sleep(engine));
            assert(db_->Put(write_options, key, set_value).ok());
            assert(db_->Get(read_options, key, &get_value).ok());
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep));
        }
    }

    void RunStatistics() {
        statistics_thread_ = std::move(std::thread(std::bind(&RocksDB::FlushMetrics, this)));
    }

    void FlushMetrics() {
        while (!statistics_stop_) {
            rocksdb_statistics_.FlushMetrics(*db_, "test_db", db_cfs_);
            std::this_thread::sleep_for(std::chrono::seconds(2));
            sys_statistics_.FlushMetrics(dbpath_);
        }
    }

private:
    void init_options() {
        options_.create_if_missing = true;
        options_.create_missing_column_families = true;
        options_.write_buffer_size = 64 * MB;

        options_.level0_file_num_compaction_trigger = 3;
        options_.level0_slowdown_writes_trigger = 5;
        options_.level0_stop_writes_trigger = 8;
        options_.max_write_buffer_number = 4;
        options_.compression_per_level = std::vector<rocksdb::CompressionType>{
                rocksdb::CompressionType::kNoCompression,
                rocksdb::CompressionType::kNoCompression,
                rocksdb::CompressionType::kNoCompression,
                rocksdb::CompressionType::kNoCompression,
                rocksdb::CompressionType::kNoCompression,
                rocksdb::CompressionType::kNoCompression,
                rocksdb::CompressionType::kNoCompression
        };

        options_.max_subcompactions = 2; // level0 --> level1 compaction
        options_.max_background_compactions = 4;
        options_.wal_bytes_per_sync = 0;
        options_.WAL_ttl_seconds = 0;
        options_.WAL_size_limit_MB = 0;
        options_.max_open_files = -1;
        options_.level_compaction_dynamic_level_bytes = true;
        options_.allow_concurrent_memtable_write = true;
        options_.enable_write_thread_adaptive_yield = true;

        options_.statistics = rocksdb::CreateDBStatistics();
        options_.listeners.push_back(statistics_event_listener_);
    }

    void init_cf_name_cache_size() {
        cf_name_cache_size_.push_back(std::make_pair(rocksdb::kDefaultColumnFamilyName, 1024));
        cf_name_cache_size_.push_back(std::make_pair("meta_cf", 1024));
        cf_name_cache_size_.push_back(std::make_pair("data_cf", 2048));
    }


    void open() {
        rocksdb::Status s = rocksdb::DB::Open(options_, dbpath_, create_column_families(), &db_cfs_, &db_);
        assert(s.ok());
    }

    std::vector<rocksdb::ColumnFamilyDescriptor> create_column_families() {
        std::vector<rocksdb::ColumnFamilyDescriptor> db_cf_vec;
        for (auto pair : cf_name_cache_size_) {
            std::shared_ptr<rocksdb::Cache> cache = rocksdb::NewLRUCache(pair.second);
            rocksdb::BlockBasedTableOptions table_options;
            table_options.block_cache = cache;
            table_options.block_size =  64 * KB;
            auto options = options_;
            options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
            auto column_options = rocksdb::ColumnFamilyOptions(options);
            column_options.max_compaction_bytes = 2 * GB; //limit for this limited will to compact
            db_cf_vec.push_back(rocksdb::ColumnFamilyDescriptor(
                    pair.first,
                    column_options));
        }
        return db_cf_vec;
    }

private:
    std::string dbpath_;
    rocksdb::Options options_;
    rocksdb::DB *db_;
    std::vector<std::pair<std::string, size_t>> cf_name_cache_size_;
    std::vector<rocksdb::ColumnFamilyHandle *> db_cfs_;
    PrometheusService  metrics_service_;
    SystemStatistics   sys_statistics_;
    RocksdbStatistics  rocksdb_statistics_;
    std::thread statistics_thread_;
    bool        statistics_stop_;
    std::shared_ptr<StatisticsEventListener>  statistics_event_listener_;
};


int main() {
    RocksDB db("./testdb", "0.0.0.0:8080");
    db.RunTest();


    return 0;
}
