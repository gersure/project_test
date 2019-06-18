//
// Created by zhengcf on 2019-06-14.
//

#pragma once


#include <cstddef>
#include <functional>
#include "generator.hh"
#include "rocksdb/db.h"
#include "metrics.hh"
#include <prometheus/exposer.h>
#include <prometheus/registry.h>


class Benchmark : public BaseMetrics {
public:
    Benchmark(uint64_t nums, int value_size, bool sync = true, bool disable_wal = false)
            : sync_(sync), disable_wal_(disable_wal), value_size_(value_size), write_nums_(nums),
              ROCKSDB_OPERATOR_METRICS(prometheus::BuildCounter()
                                               .Name("rocksdb_operator")
                                               .Help("rocksdb operator command counter")
                                               .Register(*registry_)
                                               .LabelNames({"type"})),
              ROCKSDB_OPERATOR_DURATION(prometheus::BuildHistogram()
                                                .Name("rocksdb_operator_time")
                                                .Help("rocksdb operator command time histogram")
                                                .Register(*registry_)
                                                .LabelNames({"type"},
                                                            prometheus::Histogram::ExponentialBuckets(0.5, 2.0, 20))) {

    }

    void DoPut(WriteMode write_mode,
               rocksdb::DB *db,
               rocksdb::ColumnFamilyHandle *db_cf) {
        KeyGenerator key_generator(write_mode, write_nums_);
        RandomGenerator value_generator(value_size_);
        rocksdb::WriteOptions write_options;
        write_options.sync = sync_;
        write_options.disableWAL = disable_wal_;
        auto &metrics_counter = ROCKSDB_OPERATOR_METRICS.WithLabelValues({"put"});
        auto &metrics_duration = ROCKSDB_OPERATOR_DURATION.WithLabelValues({"put"});
        size_t count_sum = 0;
        while (true) {
            auto now = std::chrono::system_clock::now();
            auto s = db->Put(write_options, db_cf,
                    std::to_string(key_generator.Next()),
                    value_generator.Generate(value_size_));
            assert(s.ok());
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now() - now);
            count_sum++;
            if (count_sum == 50) {
                metrics_counter.Increment(count_sum);
                count_sum = 0;
            }
            metrics_duration.Observe(duration.count());
        }
    }

    void Put(int thread_num, rocksdb::DB *db,
             rocksdb::ColumnFamilyHandle *db_cf,
             WriteMode write_mode = RANDOM) {
        for (int i = 0; i < thread_num; i++)
            threads_.push_back(std::thread(std::bind(&Benchmark::DoPut, this, write_mode, db, db_cf)));
    }


    void DoBatchPut(int batch_nums,
                    WriteMode write_mode,
                    rocksdb::DB *db,
                    rocksdb::ColumnFamilyHandle *db_cf) {
        KeyGenerator key_generator(write_mode, write_nums_);
        RandomGenerator value_generator(value_size_);
        rocksdb::WriteOptions write_options;
        write_options.sync = sync_;
        write_options.disableWAL = disable_wal_;
        auto &metrics_counter = ROCKSDB_OPERATOR_METRICS.WithLabelValues({"put"});
        auto &metrics_duration = ROCKSDB_OPERATOR_DURATION.WithLabelValues({"put"});
        size_t count_sum = 0;
        while (true) {
            auto now = std::chrono::system_clock::now();
            rocksdb::WriteBatch batch;
            for (int i = 0; i< batch_nums; i++) {
                batch.Put(db_cf, std::to_string(key_generator.Next()),
                          value_generator.Generate(value_size_));
            }
            auto s = db->Write(write_options, &batch);
            assert(s.ok());
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now() - now);
            count_sum+= batch_nums;
            if (count_sum > 100) {
                metrics_counter.Increment(count_sum);
                count_sum = 0;
            }
            metrics_duration.Observe(duration.count());
        }
    }

    void BenchPut(int thread_num, int batch_nums, rocksdb::DB *db,
                  rocksdb::ColumnFamilyHandle *db_cf,
                  WriteMode write_mode = RANDOM) {
        for (int i = 0; i < thread_num; i++)
            threads_.push_back(std::thread(std::bind(&Benchmark::DoBatchPut, this,
                                                     batch_nums, write_mode, db, db_cf)));
    }

    void Join() {
        for (auto &thread : threads_)
            thread.join();
    }


private:
    bool sync_;
    bool disable_wal_;
    int value_size_;
    uint64_t write_nums_;
    std::vector<std::thread> threads_;
    prometheus::Family<prometheus::Counter> &ROCKSDB_OPERATOR_METRICS;
    prometheus::Family<prometheus::Histogram> &ROCKSDB_OPERATOR_DURATION;
};



