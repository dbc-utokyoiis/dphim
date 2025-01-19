#pragma once

#include <cassert>
#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

extern "C" {
#include <sys/resource.h>
#include <sys/time.h>
}

namespace dphim {
struct TimeMeasure {
    using clock = std::chrono::system_clock;

    struct Data {
        clock::time_point prev_ed_tp;
        clock::time_point tp;
        struct rusage rusage;
        std::vector<long long> hc;
    };

    explicit TimeMeasure();
    ~TimeMeasure();

    void start();
    void point(const std::string &name);
    void print(std::ostream &out, bool json_format = false) const;

    void register_thread();

    auto totalTime() const {
        return data.back().second.tp - data.front().second.tp;
    }

    auto totalCpuTime() const {
        using namespace std::chrono;
        microseconds cpu_time{};
        auto tv2ch = [](const timeval &tv) {
            return microseconds(static_cast<long long>(tv.tv_sec) * 1000000 + tv.tv_usec);
        };
        for (auto it = std::next(std::begin(data)); it != std::end(data); ++it) {
            const auto &now = it->second;
            const auto &prev = std::prev(it)->second;
            auto stime = tv2ch(now.rusage.ru_stime) - tv2ch(prev.rusage.ru_stime);
            auto utime = tv2ch(now.rusage.ru_utime) - tv2ch(prev.rusage.ru_utime);
            cpu_time += (stime + utime);
        }
        return cpu_time;
    }

private:
    std::mutex mtx;
    std::vector<int> threads_eventset;
    std::vector<std::pair<std::string, Data>> data;
};
}// namespace dphim
