//
// Created by zhengcf on 2019-05-30.
//

#pragma once

#include <prometheus/exposer.h>
#include <prometheus/registry.h>


inline
void throw_system_error_on(bool condition, const char *what_arg) {
    if (condition) {
        throw std::system_error(errno, std::system_category(), what_arg);
    }
}

class BaseMetrics {
public:
    BaseMetrics():
            registry_(std::make_shared<prometheus::Registry>()) {}
    virtual ~BaseMetrics() = default;
    std::weak_ptr<prometheus::Registry> GetRegistry() {
        return std::weak_ptr<prometheus::Registry>(registry_);
    }

protected:
    std::shared_ptr<prometheus::Registry> registry_;
};



class PrometheusService {
public:
    PrometheusService(const std::string &host)
            : exposer_(host) {
    }

    template <typename T=std::weak_ptr<prometheus::Registry>>
    void RegisterCollectableV2(const T t) {
        exposer_.RegisterCollectable(t);
    }

    template <typename T=std::weak_ptr<prometheus::Registry>, typename... Args>
    void RegisterCollectableV2(const T t, const Args... args) {
        exposer_.RegisterCollectable(t);
        RegisterCollectableV2(args...);
    }


    void RegisterCollectable(const std::weak_ptr<prometheus::Registry> regis)
    {
        exposer_.RegisterCollectable(regis);
    }

private:
    prometheus::Exposer exposer_;
};
