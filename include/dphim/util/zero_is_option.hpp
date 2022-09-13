#pragma once

#include <atomic>
#include <optional>

namespace dphim {
template<typename T>
struct ZeroIsOption {

    ZeroIsOption() = default;
    ZeroIsOption(T val) : val(val) {}
    ZeroIsOption(std::nullopt_t) : val(0) {}

    explicit operator bool() const { return val != 0; }

    ZeroIsOption &operator=(const ZeroIsOption &) = default;
    ZeroIsOption &operator=(T v) {
        val = v;
        return *this;
    }

    ZeroIsOption &operator=(std::nullopt_t) {
        val = 0;
        return *this;
    }

    bool has_value() const { return val != 0; }

    const T &operator*() const {
        if (val == 0) {
            throw std::bad_optional_access{};
        }
        return val;
    }
    T &operator*() {
        if (val == 0) {
            throw std::bad_optional_access{};
        }
        return val;
    }

    void insert_or_add(T v) {
        val += v;
    }

    void atomic_insert_or_add(T v, std::memory_order order = std::memory_order_seq_cst) {
        reinterpret_cast<std::atomic<T> &>(val).fetch_add(v, order);
    }

private:
    T val = 0;
};

}// namespace dphim