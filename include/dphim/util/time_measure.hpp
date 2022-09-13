#pragma once

#include <cassert>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

extern "C" {
#include <sys/resource.h>
#include <sys/time.h>
}

namespace dphim {
struct TimeMeasure {

    using clock = std::chrono::system_clock;

    explicit TimeMeasure() = default;

    void start() {
        point("run_worker");
    }

    void point(const std::string &name) {
        rusage rusage{};
        getrusage(RUSAGE_SELF, &rusage);
        data.emplace_back(name, Data{.tp = clock::now(), .rusage = rusage});
    }

    void print(std::ostream &out) const {
        using namespace std::chrono;
        auto tv2ch = [](const timeval &tv) { return microseconds(static_cast<long long>(tv.tv_sec) * 1000000 + tv.tv_usec); };

        std::vector<std::vector<std::string>> outputs;
        outputs.push_back({"name:", "time", "sys", "usr", "majflt", "minflt", "RSS", "nvcsw", "nivscw"});

        for (auto it = std::next(std::begin(data)); it != std::end(data); ++it) {
            const auto &now = it->second;
            const auto &prev = std::prev(it)->second;
            auto dt = now.tp - prev.tp;
            auto stime = tv2ch(now.rusage.ru_stime) - tv2ch(prev.rusage.ru_stime);
            auto utime = tv2ch(now.rusage.ru_utime) - tv2ch(prev.rusage.ru_utime);
            using TimeType = milliseconds;
            auto suffix = " ms";
            std::vector<std::string> row;
            row.push_back(it->first + ":");
            row.push_back(std::to_string(duration_cast<TimeType>(dt).count()) + suffix);
            row.push_back(std::to_string(duration_cast<TimeType>(stime).count()) + suffix);
            row.push_back(std::to_string(duration_cast<TimeType>(utime).count()) + suffix);
            row.push_back(std::to_string(now.rusage.ru_majflt - prev.rusage.ru_majflt));
            row.push_back(std::to_string(now.rusage.ru_minflt - prev.rusage.ru_minflt));
            row.push_back(std::to_string(now.rusage.ru_maxrss / 1000) + " MB");
            row.push_back(std::to_string(now.rusage.ru_nvcsw - prev.rusage.ru_nvcsw));  // 意図したコンテキストスイッチ
            row.push_back(std::to_string(now.rusage.ru_nivcsw - prev.rusage.ru_nivcsw));// 意図しないコンテキストスイッチ
            assert(outputs[0].size() == row.size());
            outputs.push_back(std::move(row));
        }

        std::vector<std::size_t> field_len(outputs[0].size());
        for (std::size_t i = 0; i < field_len.size(); ++i) {
            auto it = std::max_element(outputs.begin(), outputs.end(), [i](auto &l, auto &r) { return l[i].size() < r[i].size(); });
            field_len[i] = (*it)[i].size();
        }

        for (auto &row: outputs) {
            for (std::size_t i = 0; i < field_len.size(); ++i)
                out << std::setw(static_cast<int>(field_len[i])) << row[i] << " ";
            out << std::endl;
        }
    }

    auto totalTime() const {
        return data.back().second.tp - data.front().second.tp;
    }

    auto totalCpuTime() const {
        using namespace std::chrono;
        microseconds cpu_time{};
        auto tv2ch = [](const timeval &tv) { return microseconds(static_cast<long long>(tv.tv_sec) * 1000000 + tv.tv_usec); };
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
    struct Data {
        clock::time_point tp;
        struct rusage rusage;
    };

    std::vector<std::pair<std::string, Data>> data;
};
}// namespace dphim
