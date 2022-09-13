#pragma once

#include <condition_variable>
#include <mutex>

namespace nova {

template<typename TState = bool>
struct cv_synchronizer {

    void wait(TState expected) {
        std::unique_lock lk(mtx);
        cv.wait(lk, [this, expected] { return st = expected; });
    }

    void notify(TState new_st) {
        std::unique_lock lk(mtx);
        st = new_st;
        cv.notify_one();
    }

    void notify_all(TState new_st) {
        std::unique_lock lk(mtx);
        st = new_st;
        cv.notify_all();
    }

    auto state() const noexcept { return st; }

private:
    std::mutex mtx;
    std::condition_variable cv;
    TState st = {};
};

template<>
struct cv_synchronizer<bool> {

    void wait(bool expected = true) {
        std::unique_lock lk(mtx);
        cv.wait(lk, [this, expected] { return st = expected; });
    }

    void notify(bool new_st = true) {
        std::unique_lock lk(mtx);
        st = new_st;
        cv.notify_one();
    }

    void notify_all(bool new_st = true) {
        std::unique_lock lk(mtx);
        st = new_st;
        cv.notify_all();
    }

    auto state() noexcept {
        std::unique_lock lk(mtx);
        return st;
    }

private:
    std::mutex mtx;
    std::condition_variable cv;
    bool st = false;
};

struct futex_synchronizer {

    void wait() {
        while (!flag.test(std::memory_order_acquire)) {
            flag.wait(false, std::memory_order_acquire);
        }
    }

    void notify() {
        flag.test_and_set(std::memory_order_release);
        flag.notify_one();
    }

    void notify_all() {
        flag.test_and_set(std::memory_order_release);
        flag.notify_all();
    }

    bool state(std::memory_order m = std::memory_order_acquire) const noexcept {
        return flag.test(m);
    }

private:
    std::atomic_flag flag = {};
};

using synchronizer = futex_synchronizer;

}// namespace nova