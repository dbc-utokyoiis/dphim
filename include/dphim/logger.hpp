#pragma once

#include <atomic>
#include <fstream>
#include <list>
#include <mutex>

#include <dphim/transaction.hpp>
#include <dphim/util/time_measure.hpp>

namespace dphim {

struct Logger {
    explicit Logger(const std::string &output_path, Utility min_util, std::size_t thread_num);

    void writeOutput(const std::vector<Item> &prefix, Utility utility);
    void incCandidateCount(std::size_t n) { candidate_count += n; }

    void timer_start() { timer.start(); }
    void time_point(const std::string &name);

    void print(std::ostream &out);
    void flushOutput();

private:
    std::fstream output;
    Utility min_util;
    std::size_t candidate_count = 0, hui_count = 0;
    std::list<std::pair<std::vector<Item>, Utility>> results;
    TimeMeasure timer;
    std::size_t thread_num;
    bool output_is_null = false;
};


template<typename T = int>
struct ConcurrentCounter {
    auto local_value() {
        std::unique_lock lk(mtx);
        auto ret = std::make_shared<T>(0);
        local_list.push_back(ret);
        return ret;
    }
    T get() {
        std::unique_lock lk(mtx);
        T ret = 0;
        for (auto l: local_list) {
            ret += *l;
        }
        return ret;
    }

private:
    std::atomic<T> value;
    std::mutex mtx;
    std::vector<std::shared_ptr<T>> local_list;
};

struct ConcurrentLogger {
    explicit ConcurrentLogger(const std::string &output_path, Utility min_util, std::size_t thread_num, bool is_debug = false);

    void register_thread() {
        timer.register_thread();
    }

    void incCandidateCount(std::size_t n) {
        static thread_local auto candidate = candidate_count.local_value();
        *candidate += n;
    }

    void addMalloc(std::size_t n) {
        if (is_debug) {
            static thread_local auto log = malloc_log.local_value();
            static thread_local auto m_count = malloc_count.local_value();
            *log += n;
            *m_count += 1;
        }
    }

    template<typename I>
    void writeOutput(const I &prefix, Utility utility) {
        static thread_local auto hui = hui_count.local_value();
        *hui += 1;

        if (not output_is_null) {
            static thread_local auto tid = res_tid.fetch_add(1, std::memory_order_relaxed);
            static thread_local auto &result = results.at(tid);
            result.emplace_back(std::make_pair(std::vector<Item>(prefix.begin(), prefix.end()), utility));
        }
    }

    void timer_start() {
        if (is_debug) {
            std::cerr << "timer start" << std::endl;
        }
        timer.start();
    }
    void time_point(const std::string &name);

    void print(std::ostream &out);
    void print_json(std::ostream &out);
    void flushOutput();

private:
    std::fstream output;

protected:
    Utility min_util;
    std::size_t thread_num;
    bool output_is_null = false;
    bool is_debug = false;

private:
    ConcurrentCounter<std::size_t> candidate_count, hui_count;
    std::atomic<std::size_t> res_tid = 0;
    std::vector<std::list<std::pair<std::vector<Item>, Utility>>> results;
    ConcurrentCounter<std::size_t> malloc_log, malloc_count;
    TimeMeasure timer;
};

}// namespace dphim
