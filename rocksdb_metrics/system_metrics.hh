//
// Created by zhengcf on 2019-05-30.
//

#pragma once

#include "metrics.hh"

class SystemStatistics : public BaseMetrics {
public:
    SystemStatistics();

    void
    FlushMetrics(const std::string& path);

public:

#define _sys_make_counter_family(val) \
//    val(STORE_ENGINE_CACHE_EFFICIENCY_VEC,      "engine_cache_efficiency",      "Efficiency of rocksdb's block cache",                  "db", "type") \


#define _sys_make_gauge_family(val) \
    val(STORE_SIZE_GAUGE_VEC,    "store_size_bytes", "Size of storage.", "type") \


#define _sys_make_histogram_family(val) \
//    val(STORE_ENGINE_COMPACTION_DURATIONS_VEC,  "engine_compaction_duration_seconds",   "Histogram of compaction duration seconds", {"db", "cf"}, prometheus::Histogram::ExponentialBuckets(0.005, 2.0, 20))

private:

#define _sys_make_counter_params(param, name, help, label, ...)   \
    prometheus::Family<prometheus::Counter>& param;
    _sys_make_counter_family(_sys_make_counter_params)
#define _sys_make_gauge_params(param, name, help, label, ...)   \
    prometheus::Family<prometheus::Gauge>& param;
    _sys_make_gauge_family(_sys_make_gauge_params)
#define _sys_make_histogram_familys_param(param, name, help, label, ...)   \
    prometheus::Family<prometheus::Histogram>& param;
    _sys_make_histogram_family(_sys_make_histogram_familys_param)

    int dummy_;
};

