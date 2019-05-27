#include <iostream>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>

#include "metric.hh"


class RocksDB {
    static const size_t MB = 1024 * 1024;
public:
    RocksDB(const std::string &dbpath) : dbpath_(dbpath) {
        init_options();
        init_cf_name_cache_size();
        open();
    }

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

        options_.max_background_compactions = 4;
        options_.wal_bytes_per_sync = 0;
        options_.WAL_ttl_seconds = 0;
        options_.WAL_size_limit_MB = 0;
        options_.max_open_files = -1;
        options_.level_compaction_dynamic_level_bytes = true;
        options_.allow_concurrent_memtable_write = true;
        options_.enable_write_thread_adaptive_yield = true;
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
            table_options.block_size = 1024 * 64;
            auto options = options_;
            options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
            db_cf_vec.push_back(rocksdb::ColumnFamilyDescriptor(
                    pair.first,
                    rocksdb::ColumnFamilyOptions(options)));
        }
        return db_cf_vec;
    }

private:
    std::string dbpath_;
    rocksdb::Options options_;
    rocksdb::DB *db_;
    std::vector<std::pair<std::string, size_t>> cf_name_cache_size_;
    std::vector<rocksdb::ColumnFamilyHandle *> db_cfs_;
};


int main() {
    RocksDB db("./testdb");


    return 0;
}