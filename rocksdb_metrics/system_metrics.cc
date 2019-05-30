//
// Created by zhengcf on 2019-05-30.
//

#include <sys/statvfs.h>
#include "system_metrics.hh"

SystemStatistics::SystemStatistics()
        :
#define _sys_init_counter_familys(param, name, help, label, ...)   \
    param(prometheus::BuildCounter() \
    .Name(name) \
    .Help(help) \
    .Register(*registry_) \
    .LabelNames({label, __VA_ARGS__})),
        _sys_make_counter_family(_sys_init_counter_familys)

#define _sys_init_gauge_familys(param, name, help, label, ...)   \
    param(prometheus::BuildGauge() \
    .Name(name) \
    .Help(help) \
    .Register(*registry_) \
    .LabelNames({label, __VA_ARGS__})),
        _sys_make_gauge_family(_sys_init_gauge_familys)

#define _sys_init_histogram_familys(param, name, help, label, ...)   \
    param(prometheus::BuildHistogram() \
    .Name(name) \
    .Help(help) \
    .Register(*registry_) \
    .LabelNames(label, __VA_ARGS__)),
        _sys_make_histogram_family(_sys_init_histogram_familys)

        dummy_() {
}

void SystemStatistics::FlushMetrics(const std::string& path) {
    class VfsSzie {
    public:
        VfsSzie(const std::string& path) : path_(path) {
            int ret = statvfs(path.c_str(), &vfs_);
            throw_system_error_on(ret == -1, "statvfs");
        }

        fsblkcnt_t Capacity() {
            return vfs_.f_bsize * vfs_.f_blocks;
        }

        fsblkcnt_t Available() {
            return vfs_.f_bsize * vfs_.f_bavail;
        }

    private:
        std::string    path_;
        struct statvfs vfs_;
    };

    VfsSzie vfs_stats(path);
    double capacity = vfs_stats.Capacity();
    double available = vfs_stats.Available();

    STORE_SIZE_GAUGE_VEC
            .WithLabelValues({"capacity"})
            .Set(capacity);
    STORE_SIZE_GAUGE_VEC
            .WithLabelValues({"available"})
            .Set(available);

}
