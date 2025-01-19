#include <dphim/util/time_measure.hpp>

extern "C" {
#include <papi.h>
#include <sys/resource.h>
#include <sys/time.h>
}

#include <iomanip>

namespace dphim {

std::vector<const char *> events = {
        "perf::NODE-LOADS",
        "perf::NODE-LOAD-MISSES",
        // "perf::NODE-STORES",
        // "perf::NODE-STORE-MISSES",
        // "perf::NODE-PREFETCHES",
        // "perf::NODE-PREFETCH-MISSES",
        "perf::CACHE-MISSES",
        "MEM_LOAD_L3_MISS_RETIRED:REMOTE_DRAM",
        "MEM_LOAD_L3_MISS_RETIRED:LOCAL_DRAM",
};

TimeMeasure::TimeMeasure() {
    if (PAPI_library_init(PAPI_VER_CURRENT) != PAPI_VER_CURRENT) {
        std::cerr << "PAPI library init error" << std::endl;
        std::exit(-1);
    }

    PAPI_library_init(PAPI_VER_CURRENT);

    if (PAPI_thread_init((unsigned long (*)(void))(pthread_self)) != PAPI_OK) {
        std::cerr << "PAPI thread initialization failed" << std::endl;
        std::exit(-1);
    }
}

void TimeMeasure::register_thread() {
    std::lock_guard lk(mtx);
    PAPI_register_thread();
    int eventSet = PAPI_NULL;
    if (int ret = PAPI_create_eventset(&eventSet); ret != PAPI_OK) {
        std::cerr << "failed to PAPI_create_eventset: " << PAPI_strerror(ret) << std::endl;
        std::exit(-1);
    }
    for (const char *ev: events) {
        if (int ret = PAPI_add_named_event(eventSet, ev); ret != PAPI_OK) {
            std::cerr << "failed to add event: " << PAPI_strerror(ret) << " " << ev << std::endl;
            std::exit(-1);
        }
    }
    threads_eventset.push_back(eventSet);
}

TimeMeasure::~TimeMeasure() {
    PAPI_shutdown();
}

void TimeMeasure::start() {
    {
        std::lock_guard lk(mtx);
        for (auto es: threads_eventset) {
            if (int ret = PAPI_start(es); ret != PAPI_OK) {
                std::cerr << "failed to PAPI_start(): " << PAPI_strerror(ret) << std::endl;
                std::exit(-1);
            }
        }
    }
    point("run_worker");
}

void TimeMeasure::point(const std::string &name) {

    auto prev_ed = clock::now();

    rusage rusage{};
    getrusage(RUSAGE_SELF, &rusage);

    std::vector<long long> hc(events.size(), 0);
    std::vector<long long> tmp_hc(events.size(), 0);

    for (auto es: threads_eventset) {
        if (PAPI_read(es, tmp_hc.data()) != PAPI_OK) {
            std::cerr << "failed to PAPI_read" << std::endl;
        }
        for (std::size_t i = 0; i < events.size(); ++i) {
            hc[i] += tmp_hc[i];
        }
    }
    data.emplace_back(name, Data{.prev_ed_tp = prev_ed,
                                 .tp = clock::now(),
                                 .rusage = rusage,
                                 .hc = hc});
}

void TimeMeasure::print(std::ostream &out, bool json_format) const {
    using namespace std::chrono;
    using TimeType = milliseconds;
    const auto time_suffix = (json_format ? "" : " ms");
    const auto mem_suffix = (json_format ? "" : " MB");

    auto tv2ch = [](const timeval &tv) {
        return microseconds(static_cast<long long>(tv.tv_sec) * 1000000 + tv.tv_usec);
    };
    std::vector<std::string> header = {"time", "sys", "usr", "majflt", "minflt", "RSS", "nvcsw", "nivscw"};
    for (auto ev: events)
        header.push_back(ev);
    std::vector<std::pair<std::string, std::vector<std::string>>> rows;

    for (auto it = std::next(std::begin(data)); it != std::end(data); ++it) {
        const auto &now = it->second;
        const auto &prev = std::prev(it)->second;
        auto dt = now.tp - prev.prev_ed_tp;
        auto stime = tv2ch(now.rusage.ru_stime) - tv2ch(prev.rusage.ru_stime);
        auto utime = tv2ch(now.rusage.ru_utime) - tv2ch(prev.rusage.ru_utime);
        std::vector<std::string> row;
        row.push_back(std::to_string(duration_cast<TimeType>(dt).count()) + time_suffix);
        row.push_back(std::to_string(duration_cast<TimeType>(stime).count()) + time_suffix);
        row.push_back(std::to_string(duration_cast<TimeType>(utime).count()) + time_suffix);
        row.push_back(std::to_string(now.rusage.ru_majflt - prev.rusage.ru_majflt));
        row.push_back(std::to_string(now.rusage.ru_minflt - prev.rusage.ru_minflt));
        row.push_back(std::to_string(now.rusage.ru_maxrss / 1000) + mem_suffix);
        row.push_back(std::to_string(now.rusage.ru_nvcsw - prev.rusage.ru_nvcsw));  // 意図したコンテキストスイッチ
        row.push_back(std::to_string(now.rusage.ru_nivcsw - prev.rusage.ru_nivcsw));// 意図しないコンテキストスイッチ

        for (std::size_t i = 0; i < events.size(); ++i) {
            row.push_back(std::to_string(now.hc[i]));
        }

        assert(header.size() == row.size());
        rows.emplace_back(it->first, std::move(row));
    }

    if (json_format) {
        // throw std::runtime_error("not implemented");
        out << "{\n";
        bool first = true;
        for (const auto &[name, elms]: rows) {
            out << (first ? "" : ",\n") << "  \"" << name << "\": {";
            for (std::size_t i = 0; i < header.size(); ++i) {
                out << "\"" << header[i] << "\": " << elms[i];
                out << (i != header.size() - 1 ? ", " : "");
            }
            out << "}";
            first = false;
        }
        out << "\n}\n";
    } else {
        std::vector<std::size_t> field_len(rows[0].second.size());
        for (std::size_t i = 0; i < field_len.size(); ++i) {
            auto it = std::max_element(rows.begin(), rows.end(),
                                       [i](auto &l, auto &r) { return l.second[i].size() < r.second[i].size(); });
            field_len[i] = std::max(header[i].size(), it->second[i].size());
        }
        auto first_len = static_cast<int>(std::max_element(rows.begin(), rows.end(),
                                                           [](auto &l, auto &r) { return l.first.size() < r.first.size(); })
                                                  ->first.size());

        out << std::setw(first_len) << "name"
            << ": ";
        for (std::size_t i = 0; i < field_len.size(); ++i)
            out << std::setw(static_cast<int>(field_len[i])) << header[i] << " ";
        out << std::endl;

        for (const auto &[name, elms]: rows) {
            out << std::setw(first_len) << name << ": ";
            for (std::size_t i = 0; i < field_len.size(); ++i)
                out << std::setw(static_cast<int>(field_len[i])) << elms[i] << " ";
            out << std::endl;
        }
    }
}

}// namespace dphim
